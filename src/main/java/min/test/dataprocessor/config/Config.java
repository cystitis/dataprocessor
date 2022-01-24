package min.test.dataprocessor.config;

import min.test.dataprocessor.DataProcessor;
import min.test.dataprocessor.config.kafka.KafkaClient;
import min.test.dataprocessor.config.kafka.KafkaConfig;
import min.test.dataprocessor.config.mysql.MysqlClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * DataProcessor 구동에 필요한 설정 정보들을 관리하는 class.
 */
public class Config {
    private static final String SEPARATOR = File.separator;
    private static final String PROPERTIES_EXTENSION = ".properties";
    private static final String JSON_EXTENSION = ".json";
    private static final String propFileName = "dataprocessor" + PROPERTIES_EXTENSION;
    private static final String schemaFileName = "schema" + JSON_EXTENSION;
    private static final String SCHEMA_NAME = "name";
    private static final String SCHEMA_FIELDS = "fields";

    private final Properties dpConfig;
    private KafkaConfig kafkaConfig;
    private KafkaClient kafkaClient;
    private MysqlClient mysqlClient;

    private Map<String, AvroSchema> schemaMap;

    public enum DP_MODE {
        ALL, PRODUCER, CONSUMER
    }

    /**
     * 설정 load 중 문제가 발생하면 동작에 치명적인 문제가 생겼다고 간주하여 system exit.
     * 구동에 필요한 설정을 담고 있기 때문에 해당 생성자는 DataProcessor 의 다른 class 들이 initialize 되기 전에 호출되어야 한다.
     * 1. load kafka config
     * 2. load mysql config
     * 3. load avro schema
     * 4. init kafka client
     * 5. init mysql client
     */
    public Config() {
        this.dpConfig = new Properties();
        try {
            String configPath = System.getProperty("user.dir") + SEPARATOR + "config";
            File configDir = new File(configPath);
            if (!configDir.exists() || !configDir.isDirectory())
                configDir.mkdirs();

            File propFile = new File(configDir.getAbsolutePath(), propFileName);
            if (!propFile.exists() || !propFile.isFile() || !propFile.getName().endsWith(PROPERTIES_EXTENSION))
                throw new IllegalArgumentException("DataProcessor configuration file is not exist... path : " + propFile.getAbsolutePath());

            File schemaFile = new File(configDir.getAbsolutePath(), schemaFileName);
            if (!schemaFile.exists() || !schemaFile.isFile() || !schemaFile.getName().endsWith(JSON_EXTENSION))
                throw new IllegalArgumentException("Schema file is not exist... path : " + schemaFile.getAbsolutePath());

            this.loadPropFile(propFile);
            this.loadSchemaFile(schemaFile);
            this.kafkaConfig = new KafkaConfig(getKafkaBootstrapServer(),
                    getKafkaPartitionNum(),
                    getMaxPollIntervalMs(),
                    getMaxPollRecords(),
                    getSessionTimeout()
            );
            this.kafkaClient = new KafkaClient(this.kafkaConfig, new ArrayList<>(this.schemaMap.values()));

            if (DP_MODE.PRODUCER == getDPMode())
                return;

            this.mysqlClient = new MysqlClient(getDbType(), getMysqlHost(), getMysqlDatabase(), getMysqlUser(), getMysqlPassword(), this.schemaMap);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("System shutdown because Invalid configuration... message : " + e.getMessage());
            System.exit(DataProcessor.INVALID_CONFIGURATION_EXIT);
        }
    }

    /**
     * dataprocessor.properties 파일을 load 한다.
     * @param propFile dataprocessor.properties
     * @throws Exception
     */
    private void loadPropFile(File propFile) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(propFile), StandardCharsets.UTF_8))) {
            this.dpConfig.load(reader);
        }
    }

    /**
     * schema.json 파일을 load 한다.
     * @param schemaFile schema.json
     * @throws Exception
     */
    private void loadSchemaFile(File schemaFile) throws Exception {
        this.schemaMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(schemaFile), StandardCharsets.UTF_8))) {
            JSONArray schemaArray = (JSONArray) new JSONParser().parse(reader);
            if (schemaArray == null)
                throw new IllegalArgumentException("Avro schema info is not exist...");

            for (Object obj : schemaArray) {
                JSONObject schemaObj = (JSONObject) obj;
                try {
                    String schemaName = (String) schemaObj.get(SCHEMA_NAME);
                    JSONObject fieldObj = (JSONObject) schemaObj.get(SCHEMA_FIELDS);
                    this.schemaMap.put(schemaName, new AvroSchema(schemaName, fieldObj));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid Avro schema info... path : " + schemaFile.getAbsolutePath());
                }
            }
        }

        new Thread(() -> {
            try {
                initSchemaFileWatch(schemaFile.getParent());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();     // schema file watcher
    }

    /**
     * Avro schema 파일을 watch.
     * 파일이 update 된 경우 schema 정보가 변경되었으므로 현재 동작중인 작업을 중료 후 새 설정으로 재기동한다.
     * @param dir schema file dir
     * @throws Exception
     */
    private void initSchemaFileWatch(String dir) throws Exception {
        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            Path watchPath = Paths.get(dir);
            watchPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            while (true) {
                WatchKey key = watchService.take();
                Thread.sleep(50);
                List<WatchEvent<?>> list = key.pollEvents();
                for (WatchEvent<?> event : list) {
                    WatchEvent.Kind<?> kind = event.kind();
                    Path context = (Path) event.context();
                    if (!"schema.json".equals(context.toString())) continue;
                    if (StandardWatchEventKinds.ENTRY_MODIFY.equals(kind)) {
                        System.out.println("schema.json is modified. DataProcessor will be restarted");
                        DataProcessor.INSTANCE.restartDataProcessor();
                    }
                }
                if (!key.reset()) break;
            }
            watchService.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getKafkaBootstrapServer() { return this.dpConfig.getProperty("kafka.bootstrap.server", "127.0.0.1:9092"); }

    private int getKafkaPartitionNum() { return Integer.parseInt(this.dpConfig.getProperty("kafka.topic.partition.num", "10")); }

    private String getMaxPollIntervalMs() { return this.dpConfig.getProperty("kafka.max.poll.interval.ms", "30000"); }

    private String getMaxPollRecords() { return this.dpConfig.getProperty("kafka.max.poll.records", "1"); }

    private String getSessionTimeout() { return this.dpConfig.getProperty("kafka.session.timeout.ms", "10000"); }

    private String getDbType() { return this.dpConfig.getProperty("db.type", "mysql"); }

    private String getMysqlHost() { return this.dpConfig.getProperty("db.host", "127.0.0.1:3306"); }

    private String getMysqlDatabase() { return this.dpConfig.getProperty("db.database", "data"); }

    private String getMysqlUser() { return this.dpConfig.getProperty("db.user", "dp"); }

    private String getMysqlPassword() { return this.dpConfig.getProperty("db.password", "dataprocessor"); }

    public DP_MODE getDPMode() {
        String mode = this.dpConfig.getProperty("dp.mode", "all");
        DP_MODE dpMode;
        try {
            dpMode = DP_MODE.valueOf(mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            dpMode = DP_MODE.ALL;
        }

        return dpMode;
    }

    public Map<String, AvroSchema> getSchemaMap() { return this.schemaMap; }

    public KafkaConfig getKafkaConfig() { return this.kafkaConfig; }

    public KafkaClient getKafkaClient() { return this.kafkaClient; }

    public MysqlClient getMysqlClient() { return this.mysqlClient; }
}