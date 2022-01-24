package min.test.dataprocessor;

import min.test.dataprocessor.config.Config;
import min.test.dataprocessor.processor.generator.DataGenerator;
import min.test.dataprocessor.processor.kafka.ConsumerManager;
import min.test.dataprocessor.processor.kafka.ProducerManager;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Application 을 구동하기 위한 Initial singleton class
 */
public enum DataProcessor {
    INSTANCE;
    public static final int INVALID_CONFIGURATION_EXIT = 1;
    public static final int DATA_PROCESSOR_INITIALIZE_EXIT = 2;
    public static final int DATA_PROCESSOR_START_EXIT = 3;

    private final Config config;
    private ProducerManager producerManager;
    private ConsumerManager consumerManager;
    private DataGenerator dataGenerator;
    private AtomicBoolean firstStart = new AtomicBoolean(true);

    /**
     * 생성자.
     * 구동에 필요한 configuration 파일을 읽어 설정을 load 한다.
     */
    DataProcessor() { this.config = new Config(); }

    /**
     * initialize DataProcessor
     * 1. init DataGenerator
     * 2. init Kafka producer
     *   2-1. create kafka topic
     * 3. init kafka consumer
     *   3-1. create mysql table
     * 4. add shutdown hook
     * @throws Exception init 중 문제가 발생한 경우 system exit.
     */
    private void init() {
        try {
            switch (this.config.getDPMode()) {
                case ALL:
                    this.dataGenerator = new DataGenerator();
                    this.producerManager = new ProducerManager();
                    this.consumerManager = new ConsumerManager();
                    break;
                case PRODUCER:
                    this.dataGenerator = new DataGenerator();
                    this.producerManager = new ProducerManager();
                    break;
                case CONSUMER:
                    this.consumerManager = new ConsumerManager();
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(DATA_PROCESSOR_INITIALIZE_EXIT);
        }

        if (this.firstStart.get()) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownAll));
            this.firstStart.set(false);
        }
    }

    /**
     * run DataProcessor
     * 1. run DataGenerator
     * 2. run Kafka producer
     * 3. run Kafka consumer
     */
    private void run() {
        try {
            if (this.dataGenerator != null) {
                this.dataGenerator.start();
                System.out.println("※ DataGenerator running complete !");
            }

            if (this.producerManager != null) {
                this.producerManager.start();
                System.out.println("※ ProducerManager running complete !");
            }

            if (this.consumerManager != null) {
                this.consumerManager.start();
                System.out.println("※ ConsumerManager running complete !");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(DATA_PROCESSOR_START_EXIT);
        }
        System.out.println("※ All Processor start complete !");
    }

    /**
     * shutdown DataProcessor
     */
    private void shutdownAll() {
        if (this.dataGenerator != null) {
            this.dataGenerator.shutdown();
            System.out.println("※ DataGenerator shutdown complete !");
        }

        if (this.producerManager != null) {
            this.producerManager.shutdown();
            System.out.println("※ ProducerManager shutdown complete !");
        }

        if (this.consumerManager != null) {
            this.consumerManager.shutdown();
            System.out.println("※ ConsumerManager shutdown complete !");
        }
        System.out.println("※ All Processor shutdown complete !");
    }

    /**
     * schema.json 파일이 수정되었을 경우 schema 를 새로 load 하여 모듈을 재시작한다
     * @throws Exception
     */
    public void restartDataProcessor() {
        System.out.println("※ Restart All Processor !");
        this.shutdownAll();
        this.init();
        this.run();
        System.out.println("※ Restart All Processor complete !");
    }

    public Config getConfig() { return this.config; }

    public ProducerManager getProducerManager() { return this.producerManager; }

    public static void main(String ... args) throws InterruptedException {
        System.out.println("※ DataProcessor initialize...");
        DataProcessor.INSTANCE.init();
        System.out.println("※ DataProcessor initialize complete !");

        DataProcessor.INSTANCE.run();

        while (!Thread.currentThread().isInterrupted()) {
            TimeUnit.SECONDS.sleep(60);
        }
    }
}

