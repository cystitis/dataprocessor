package min.test.dataprocessor.processor.generator;

import min.test.dataprocessor.DataProcessor;
import min.test.dataprocessor.config.AvroSchema;
import min.test.dataprocessor.processor.Processor;
import org.apache.avro.Schema;
import org.json.simple.JSONObject;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 로 전송할 random data 를 생성하는 class.
 * DataGenerator thread 의 수는 schema.json 의 object 수와 같다.
 */
public class DataGenerator implements Processor {
    private final Map<String, AvroSchema> schemaMap;
    private final AtomicLong generatedCount;
    private List<Worker> workerList;

    /**
     * DataGenerator thread.
     * Worker thread 의 수는 schema.json 의 object 수와 같다.
     */
    private class Worker extends Thread {
        private final AvroSchema schema;

        Worker(String name, AvroSchema schema) {
            super(name);
            this.schema = schema;
        }

        /**
         * schema 의 field type 에 맞는 랜덤 data 를 생성한다.
         * @param schema Avro schema
         * @return generated data
         */
        @SuppressWarnings("unchecked")
        private JSONObject generateValue(Schema schema) {
            JSONObject generated = new JSONObject();
            String value;

            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                String fieldType = field.schema().getName();

                int rand = new Random().nextInt(100000);
                if (fieldName.contains(AvroSchema.FIELD_TYPE_TIME_STAMP)) {
                    value = String.valueOf(System.currentTimeMillis());
                } else if (AvroSchema.FIELD_TYPE_STRING.equals(fieldType)) {
                    value = fieldName.toUpperCase() + "-" + rand;
                } else if (AvroSchema.FIELD_TYPE_INTEGER.equals(fieldType)) {
                    value = String.valueOf(rand);
                } else if (AvroSchema.FIELD_TYPE_LONG.equals(fieldType)) {
                    value = String.valueOf(rand);
                } else if (AvroSchema.FIELD_TYPE_DOUBLE.equals(fieldType)) {
                    double dRand = new Random().nextDouble();
                    value = String.valueOf(dRand);
                } else {
                    value = "INVALID_VALUE";
                }
                generated.put(fieldName, value);
            }

            return generated;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            try {
                while (true) {
                    JSONObject generated = this.generateValue(this.schema.getSchema());
                    if (DataProcessor.INSTANCE.getProducerManager() == null)
                        continue;

                    DataProcessor.INSTANCE.getProducerManager().put(this.schema.getSchemaName(), generated);
                    generatedCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void shutdown() { this.interrupt(); }
    }

    public DataGenerator() {
        this.generatedCount = new AtomicLong(0L);
        this.schemaMap = DataProcessor.INSTANCE.getConfig().getSchemaMap();
        this.initWorkers(this.schemaMap.size());
    }

    /**
     * DataGenerator thread 를 initialize 한다.
     * DataGenerator thread 의 수는 schema.json 의 object 수와 같다.
     * @param size thread 개수
     */
    @Override
    public void initWorkers(int size) {
        this.workerList = new ArrayList<>();
        for (AvroSchema schema : this.schemaMap.values()) {
            Worker worker = new Worker("Generator-" + schema.getSchemaName(), schema);
            this.workerList.add(worker);
        }
        System.out.println("Initialize Data generator complete ! (size : " + this.workerList.size() + ")");
    }

    /**
     * initialize 가 완료된 DataGenerator 를 실행한다.
     */
    @Override
    public void start() {
        this.workerList.forEach(Worker::start);
    }

    /**
     * 동작중인 DataGenerator thread 들을 종료한다.
     */
    @Override
    public void shutdown() {
        this.workerList.forEach(Worker::shutdown);
        System.out.println("Shutdown data generator...");
    }
}
