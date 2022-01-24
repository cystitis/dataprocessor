package min.test.dataprocessor.config.kafka;

import min.test.dataprocessor.config.AvroSchema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Kafka client 를 관리하는 class.
 * client 를 통해 topic create, data insert 를 수행한다.
 */
public class KafkaClient {
    private final List<AvroSchema> schemaList;
    private final KafkaConfig kafkaConfig;
    private final AdminClient client;

    /**
     * kafka client init 에 실패한 경우 동작에 치명적인 문제로 간주하여 system exit.
     * @param kafkaConfig kafka config
     * @param schemaList object list of schema.json
     */
    public KafkaClient(KafkaConfig kafkaConfig, List<AvroSchema> schemaList) {
        this.kafkaConfig = kafkaConfig;
        this.schemaList = schemaList;
        this.client = this.init();
    }

    /**
     * create client.
     * @return kafka client
     */
    private AdminClient init() {
        System.out.println("KAFKA client initialize complete... host : " + this.kafkaConfig.getBootstrapServer());
        return AdminClient.create(this.kafkaConfig.getProducerConf());
    }

    /**
     * topic 을 생성한다.
     * 이미 생성되어 있다면 skip 한다.
     * @throws Exception topic 생성에 문제가 발생하면 동작에 치명적인 문제로 간주하여 프로그램을 종료한다.
     */
    public void createTopic() throws Exception {
        ListTopicsResult listResult = this.client.listTopics();
        for (AvroSchema schema : this.schemaList) {
            String schemaName = schema.getSchemaName();
            try {
                if (listResult.names().get().contains(schemaName))
                    continue;

                short replicationNum = 1;
                NewTopic topic = new NewTopic(schemaName, this.kafkaConfig.getPartitionNum(), replicationNum);
                CreateTopicsResult topicResult = this.client.createTopics(Collections.singletonList(topic));
                if (topicResult == null || topicResult.values() == null || !topicResult.values().containsKey(schemaName))
                    throw new Exception("Failed to create topic... topicName : " + schemaName);

                while (!topicResult.values().get(schemaName).isDone()) {
                    System.out.println("Waiting for create topic... topicName : " + schemaName);
                    Thread.sleep(100);
                }
                System.out.println("Create topic complete... topicName : " + schemaName + ", partitions number : " + this.kafkaConfig.getPartitionNum());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
