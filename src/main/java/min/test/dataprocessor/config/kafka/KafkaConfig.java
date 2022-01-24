package min.test.dataprocessor.config.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka producer 및 consumer 의 설정 정보를 담고 있는 class.
 */
public class KafkaConfig {
    private final int partitionNum;
    private final String bootstrapServer;
    private final Properties producerConf;
    private final Properties consumerConf;
    private final String maxPollInterval;
    private final String maxPollRecords;
    private final String sessionTimeout;

    public KafkaConfig(String bootstrapServer, int partitionNum, String maxPollInterval, String maxPollRecords, String sessionTimeout) {
        this.partitionNum = partitionNum;
        this.bootstrapServer = bootstrapServer;
        this.producerConf = new Properties();
        this.consumerConf = new Properties();
        this.maxPollInterval = maxPollInterval;
        this.maxPollRecords = maxPollRecords;
        this.sessionTimeout = sessionTimeout;
        this.initProducerConfig();
        this.initConsumerConfig();
    }

    private void initProducerConfig() {
        this.producerConf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer.replaceAll(" ", ""));
        this.producerConf.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        this.producerConf.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        this.producerConf.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");
        this.producerConf.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "268435456");
        this.producerConf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producerConf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producerConf.setProperty("producer.type", "async");
    }

    private void initConsumerConfig() {
        this.consumerConf.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer.replaceAll(" ", ""));
        this.consumerConf.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumerConf.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, this.maxPollInterval);         // polling 후 commit 까지 대기 시간
        this.consumerConf.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.maxPollRecords);              // poll() 호출 시 가져오는 최대 records 수
        this.consumerConf.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.sessionTimeout);
        this.consumerConf.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumerConf.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumerConf.getProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
    }

    public String getBootstrapServer() { return this.bootstrapServer; }

    public int getPartitionNum() { return this.partitionNum; }

    public Properties getProducerConf() { return this.producerConf; }

    /**
     * consumer 정보는 consumer group 마다 group 명이 변경될 수 있기 때문에 복제하여 사용한다.
     * @return
     */
    public Properties getConsumerConf() {
        Properties props = new Properties();
        for (Object key : this.consumerConf.keySet()) {
            props.setProperty((String) key, (String) this.consumerConf.get(key));
        }

        return props;
    }
}
