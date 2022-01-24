package min.test.dataprocessor.processor.kafka;

import min.test.dataprocessor.DataProcessor;
import min.test.dataprocessor.config.Config;
import min.test.dataprocessor.config.AvroSchema;
import min.test.dataprocessor.processor.Processor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Producer 를 관리하는 class.
 * DataGenerator 에서 생성된 Avro schema data 를 Kafka broker 로 전송한다.
 */
public class ProducerManager implements Processor {
    private final Config config;
    private final SynchronousQueue<Map.Entry<String, JSONObject>> queue;
    private AtomicLong producedCount;
    private Producer<String, byte[]> producer;
    private List<Worker> workerList;

    /**
     * Producer thread.
     * Worker thread 의 수는 topic 의 partition 수와 같다.
     */
    private class Worker extends Thread {
        Worker(String name) {
            super(name);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Map.Entry<String, JSONObject> dataEntry = queue.take();
                    String schemaName = dataEntry.getKey();
                    JSONObject data = dataEntry.getValue();
                    send(schemaName, data.toJSONString().getBytes());
                    producedCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                // shutdown. do nothing.
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }
        }

        private void shutdown() { this.interrupt(); }
    }

    public ProducerManager() throws Exception {
        this.config = DataProcessor.INSTANCE.getConfig();
        this.producer = new KafkaProducer<>(this.config.getKafkaConfig().getProducerConf());
        this.queue = new SynchronousQueue<>(true);
        this.createTopic();
        this.initWorkers(this.config.getKafkaConfig().getPartitionNum());

        this.producedCount = new AtomicLong(0);
        new Thread(() -> {      // cnt thread
            while (true) {
                System.out.println("Producer CNT : " + getCnt());
                try { Thread.sleep(10 * 1000); } catch (InterruptedException e) { e.printStackTrace(); }
            }
        }).start();
    }

    /**
     * schema.json 의 schema 이름과 같은 topic 을 생성한다.
     * 이미 topic 이 생성되어 있으면 skip 한다.
     * @throws Exception topic 생성 중 문제가 발생하면 동작에 치명적인 문제로 간주하고 프로그램을 종료한다.
     */
    private void createTopic() throws Exception {
        this.config.getKafkaClient().createTopic();
    }

    /**
     * DataGenerator 에서 생성된 data 를 Producing 하기 전 queue 에 put 한다.
     * @param schemaName
     * @param data
     * @throws InterruptedException
     */
    public void put(String schemaName, JSONObject data) throws InterruptedException {
        this.queue.put(new AbstractMap.SimpleEntry<>(schemaName, data));
    }

    /**
     * Kafka 로 data 를 전송한다.
     * @param topicName topic name
     * @param data generated data
     */
    private void send(String topicName, byte[] data) {
        if (data == null || data.length <= 0) return;

        producer.send(new ProducerRecord<>(topicName, data), (metaData, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
        });
    }

    public long getCnt() { return this.producedCount.getAndSet(0); }

    /**
     * Producer thread 를 initialize 한다.
     * Producer thread 의 수는 topic 의 partition 수와 같다.
     * @param size thread 개수
     */
    @Override
    public void initWorkers(int size) {
        this.workerList = new ArrayList<>();
        for (int partitionCnt = 1; partitionCnt <= size; partitionCnt++) {
            Worker worker = new Worker("Producer-" + partitionCnt);
            this.workerList.add(worker);
        }
        System.out.println("Initialize Producer complete ! (size : " + this.workerList.size() + ")");
    }

    /**
     * initialize 가 완료된 Producer 들을 실행한다.
     */
    @Override
    public void start() {
        this.workerList.forEach(Worker::start);
    }

    /**
     * 동작중인 Producer thread 들을 종료한다.
     */
    @Override
    public void shutdown() {
        this.workerList.forEach(Worker::shutdown);
        System.out.println("Shutdown ProducerManager...");
    }
}
