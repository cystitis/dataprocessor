package min.test.dataprocessor.processor.kafka;

import min.test.dataprocessor.DataProcessor;
import min.test.dataprocessor.config.kafka.KafkaConfig;
import min.test.dataprocessor.config.mysql.MysqlClient;
import min.test.dataprocessor.config.AvroSchema;
import min.test.dataprocessor.processor.Processor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Consumer 를 관리하는 class.
 * Kafka cluster 의 data 를 consuming 하여 DB 로 insert 한다.
 */
class ConsumerGroup implements Processor {
    private final String groupName;
    private final String topicName;
    private final AvroSchema schema;
    private final Properties consumerConf;
    private final MysqlClient client;
    private final List<Worker> workerList;

    private AtomicLong consumedCount;
    private AtomicBoolean isGroupShutdown = new AtomicBoolean(false);

    /**
     * Consumer thread.
     * Worker thread 의 수는 topic 의 partition 수와 같다.
     */
    private class Worker extends Thread {
        private final Consumer<String, byte[]> consumer;
        private ConsumerRecords<String, byte[]> records;
        private boolean committed = false;
        private AtomicBoolean shutdown = new AtomicBoolean(false);
        private AtomicBoolean isWorking = new AtomicBoolean(true);

        Worker(String name) {
            super(name);
            this.consumer = new KafkaConsumer<>(consumerConf);
            this.consumer.subscribe(Collections.singletonList(topicName));
        }

        @Override
        public void run() {
            this.isWorking = new AtomicBoolean(true);
            boolean inserted = false;
            try {
                while (true) {
                    if (this.shutdown.get()) {
                        throw new WakeupException();
                    }
                    this.committed = false;
                    records = consumer.poll(Duration.ofMillis(100));
                    if (records == null || records.isEmpty()) {
                        try { Thread.sleep(100); } catch (InterruptedException interEx) { interEx.printStackTrace(); }
                        continue;
                    }

                    inserted = false;
                    for (ConsumerRecord<String, byte[]> record : records) {
                        String polled = new String(record.value(), StandardCharsets.UTF_8);
                        insert(polled);
                        inserted = true;
                        increaseCnt();
                    }

                    this.consumer.commitSync();     // insert 가 끝난 후에 명시적으로 commit 한다.
                    this.committed = true;
                }
            } catch (WakeupException e) {
                // shutdown. do nothing.
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (inserted && !this.committed) {
                    this.consumer.commitSync();
                    this.committed = true;
                } else if (!inserted && this.committed) {
                    System.err.println("[" + this.getName() + "] not inserted, but it was committed... inserted : " + inserted + ", committed : " + committed);
                }

                this.consumer.close();
                this.isWorking.set(false);
            }
        }

        private void shutdown() { this.shutdown.set(true); }

        private boolean isWorking() { return this.isWorking.get(); }
    }

    ConsumerGroup(String consumerGroupName, AvroSchema schema) throws Exception {
        this.schema = schema;
        this.topicName = this.schema.getSchemaName();
        this.workerList = new ArrayList<>();
        this.groupName = consumerGroupName;
        KafkaConfig kafkaConfig = DataProcessor.INSTANCE.getConfig().getKafkaConfig();
        Properties props = kafkaConfig.getConsumerConf();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupName);
        this.consumerConf = props;
        this.client = DataProcessor.INSTANCE.getConfig().getMysqlClient();
        this.createTable();
        this.initWorkers(kafkaConfig.getPartitionNum());
        this.consumedCount = new AtomicLong(0);
    }

    /**
     * ConsumerGroup 에 할당된 schema 이름과 같은 table 을 생성한다.
     * 이미 table 이 생성되어 있으면 skip 한다.
     * @throws Exception table 생성 중 문제가 발생하면 동작에 치명적인 문제로 간주하고 프로그램을 종료한다.
     */
    private void createTable() throws Exception {
        this.client.createTable(this.topicName);
    }

    /**
     * ConsumerGroup 에 할당된 schema 이름과 같은 table 에 insert 한다.
     * @param data consumed data
     */
    private void insert(String data) {
        this.client.insert(this.topicName, data);
    }

    private String getGroupName() { return this.groupName; }

    private void increaseCnt() { this.consumedCount.incrementAndGet(); }

    boolean isGroupShutdown() { return this.isGroupShutdown.get(); }

    long getCnt() { return this.consumedCount.getAndSet(0); }

    /**
     * ConsumerGroup 에서 사용할 consumer thread 를 initialize 한다.
     * consumer thread 의 수는 topic 의 partition 수와 같다.
     * @param size thread 개수
     */
    @Override
    public void initWorkers(int size) {
        for (int partitionCnt = 1; partitionCnt <= size; partitionCnt++) {
            Worker worker = new Worker(this.groupName + "-consumer-" + partitionCnt);
            this.workerList.add(worker);
        }
        System.out.println("Initialize " + this.getGroupName() + " Consumer thread complete ! (size : " + this.workerList.size() + ")");
    }

    /**
     * initialize 가 완료된 Consumer 들을 실행한다.
     */
    @Override
    public void start() {
        this.workerList.forEach(Worker::start);
        System.out.println("Start complete " + this.groupName);
    }

    /**
     * ConsumerGroup 에서 동작중인 consumer thread 들을 종료한다.
     * topic 에서 polling 하여 처리중인 data 가 있다면 처리가 끝나기를 기다린 후 종료한다.
     */
    @Override
    public void shutdown() {
        this.workerList.forEach(Worker::shutdown);
        boolean checkLoop = true;
        while (checkLoop) {
            boolean waitForShutdown = false;
            System.out.println("Wait for close consumer threads...");
            try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }

            for (Worker worker : this.workerList) {
                if (worker.isWorking())
                    waitForShutdown = true;

                System.out.println(worker.getName() + ", isWorking() : " + worker.isWorking() + ", waitForShutdown : " + waitForShutdown);
            }
            if (!waitForShutdown)
                checkLoop = false;
        }

        this.isGroupShutdown.set(true);
        System.out.println("Shutdown consumer threads...");
    }
}
