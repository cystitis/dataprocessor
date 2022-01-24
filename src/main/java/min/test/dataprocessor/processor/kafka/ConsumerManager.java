package min.test.dataprocessor.processor.kafka;

import min.test.dataprocessor.DataProcessor;
import min.test.dataprocessor.config.AvroSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Kafka Consumer group 을 관리하는 class.
 * Kafka cluster 의 data 를 consuming 하는 ConsumgerGroup 을 관리한다.
 * ConsumerGroup 의 수는 schema.json 의 object 수와 같다.
 */
public class ConsumerManager {
    private List<ConsumerGroup> consumerGroupList;

    public ConsumerManager() throws Exception {
        this.consumerGroupList = new ArrayList<>();
        this.initConsumerGroup();

        new Thread(() -> {      // cnt thread
            while (true) {
                long cnt = 0;
                for (ConsumerGroup group : this.consumerGroupList) {
                    cnt += group.getCnt();
                }
                System.out.println("Consumer CNT : " + cnt);

                try { Thread.sleep(10 * 1000); } catch (InterruptedException e) { e.printStackTrace(); }
            }
        }).start();
    }

    /**
     * ConsumerGroup 을 initialize 한다.
     * Consumer group 의 수는 schema.json 의 object 수와 같다.
     */
    private void initConsumerGroup() throws Exception {
        for (AvroSchema schema : DataProcessor.INSTANCE.getConfig().getSchemaMap().values()) {
            String groupName = "DataProcessor-" + schema.getSchemaName();
            this.consumerGroupList.add(new ConsumerGroup(groupName, schema));
        }
        System.out.println("Initialize Consumer group complete ! (size : " + this.consumerGroupList.size() + ")");
    }

    /**
     * initialize 가 완료된 ConsumerGroup 들을 실행한다.
     */
    public void start() {
        this.consumerGroupList.forEach(ConsumerGroup::start);
    }

    /**
     * 동작중인 Consumer group 들을 종료한다.
     * 아직 처리가 끝나지 않은 Consumer group 이 있다면 처리가 끝나기를 기다린 후 종료한다.
     */
    public void shutdown() {
        this.consumerGroupList.forEach(ConsumerGroup::shutdown);

        boolean checkLoop = true;
        while (checkLoop) {
            boolean waitForShutdown = false;
            System.out.println("Wait for close consumer group...");
            try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }

            for (ConsumerGroup group: this.consumerGroupList) {
                if (!group.isGroupShutdown())
                    waitForShutdown = true;
            }
            if (!waitForShutdown)
                checkLoop = false;
        }

        DataProcessor.INSTANCE.getConfig().getMysqlClient().close();    // db connection close
        System.out.println("Shutdown ConsumerManager...");
    }
}
