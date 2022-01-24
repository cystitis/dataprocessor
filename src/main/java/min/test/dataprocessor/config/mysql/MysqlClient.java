package min.test.dataprocessor.config.mysql;

import min.test.dataprocessor.config.AvroSchema;
import org.apache.avro.Schema.Field;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.sql.*;
import java.util.Iterator;
import java.util.Map;

/**
 * Mysql client 를 관리하는 class.
 * client 를 통해 table create, data insert 를 수행한다.
 * dataprocessor.properties 에 설정된 db.type 에 따라 connection 을 생성한다.
 */
public class MysqlClient {
    private static final String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String MARIA_DRIVER_NAME = "org.mariadb.jdbc.Driver";
    private final DB_TYPE dbType;
    private final String host;
    private final String database;
    private final String user;
    private final String password;
    private final Map<String, AvroSchema> schemaMap;
    private Connection connection;

    public enum DB_TYPE {
        MARIA, MYSQL
    }

    /**
     * db connection 에 실패한 경우 동작에 치명적인 문제로 간주하여 system exit
     * @param host host
     * @param database database
     * @param user user
     * @param password password
     * @param schemaMap Avro schema map
     * @throws Exception
     */
    public MysqlClient(String dbType, String host, String database, String user, String password, Map<String, AvroSchema> schemaMap) throws Exception {
        DB_TYPE type;
        try {
            type = DB_TYPE.valueOf(dbType.toUpperCase());
        } catch (IllegalArgumentException e) {
            type = DB_TYPE.MYSQL;
        }
        this.dbType = type;
        this.host = host;
        this.database = database;
        this.user = user;
        this.password = password;
        this.schemaMap = schemaMap;
        this.connection = this.init();
    }

    /**
     * create db connection.
     * @return db connection.
     * @throws Exception
     */
    private Connection init() throws Exception {
        if (this.connection == null) {
            String url = null;
            String database = this.database;
            switch (dbType) {
                case MYSQL:
                    url = "jdbc:mysql://" + this.host + "/";
                    Class.forName(MYSQL_DRIVER_NAME);
                    break;
                case MARIA:
                    url = "jdbc:mariadb://" + this.host + "/";
                    Class.forName(MARIA_DRIVER_NAME);
                    break;
            }
            try {
                this.connection = this.getConnection(url + database, this.user, this.password);
            } catch (SQLSyntaxErrorException e) {
                if (e.getMessage() != null && e.getMessage().contains("Unknown database")) {
                    System.out.println("Database is not exist... create database... database : " + database);
                    try {
                        this.connection = this.getConnection(url, this.user, this.password);
                        Statement stmt = this.connection.createStatement();
                        String query = "CREATE DATABASE " + database;
                        boolean rs = stmt.execute(query);
                    } catch (SQLException ex) {
                        throw new Exception("Failed to create database... url : " + url + ", user : " + user + ", password : " + password + ", message : " + e.getMessage());
                    }
                    this.connection = this.getConnection(url + database, this.user, this.password);
                }
            } catch (SQLException e) {
                throw new Exception("Invalid mysql connection info... url : " + url + ", user : " + user + ", password : " + password + ", message : " + e.getMessage());
            }
        }

        return this.connection;
    }

    /**
     * create db connection.
     * @param url url
     * @param user user
     * @param password password
     * @return db connection
     * @throws SQLException
     */
    private Connection getConnection(String url, String user, String password) throws SQLException {
        Connection connection = DriverManager.getConnection(url, user, password);
        System.out.println(this.dbType.name() + " connection complete... url : " + url + ", user : " + user);

        return connection;
    }

    /**
     * mysql table 이 없다면 생성한다
     * @param tableName schema name
     * @throws Exception table 생성에 문제가 발생하면 동작에 치명적인 문제로 간주하여 프로그램을 종료한다.
     */
    public void createTable(String tableName) throws Exception {
        String query = null;
        try (Statement stmt = this.connection.createStatement()) {
            query = this.makeCreateTableQuery(tableName);
            stmt.executeQuery(query);
        } catch (SQLException e) {
            throw new Exception("Failed to create table... table name : " + tableName + ", query : " + query + ", message : " + e.getMessage());
        }
        System.out.println("Create table complete... table name : " + tableName);
    }

    private String makeCreateTableQuery(String tableName) {
        AvroSchema schema = this.schemaMap.get(tableName);
        StringBuilder builder = new StringBuilder("(");
        Iterator<Field> iter = schema.getSchema().getFields().iterator();
        while (true) {
            Field field = iter.next();
            String fieldName = field.name();
            String fieldType = field.schema().getName();
            switch (fieldType) {
                case AvroSchema.FIELD_TYPE_INTEGER:
                    fieldType = "INT";
                    break;
                case AvroSchema.FIELD_TYPE_LONG:
                    fieldType = "BIGINT";
                    break;
                case AvroSchema.FIELD_TYPE_DOUBLE:
                    fieldType = "DOUBLE";
                    break;
                case AvroSchema.FIELD_TYPE_STRING:
                    fieldType = "TEXT";
                    break;
                case AvroSchema.FIELD_TYPE_TIME_STAMP:
                    fieldType = "TIMESTAMP";
                    break;
            }
            builder.append("`").append(fieldName).append("` ").append(fieldType);
            if (iter.hasNext())
                builder.append(", ");
            else break;
        }
        builder.append(")");

        return "CREATE TABLE IF NOT EXISTS " + tableName + builder.toString();
    }

//    public void newInsert(String tableName, String row) {
//        PreparedStatement pstmt = null;
//        ResultSet rs = null;
//        try {
//
//            String query = "INSERT INTO " + tableName + "(";
//            String keyPart = ") VALUES(";
//            String valuePart = " )";
//
//            int pos = 0;
//            JSONObject data = (JSONObject) new JSONParser().parse(row);
//            StringBuilder keys = new StringBuilder();
//            StringBuilder values = new StringBuilder();
//            List<String> valueList = new ArrayList<>();
//            for (Object key : data.keySet()) {
//                pos++;
//                String fieldName = (String) key;
//                valueList.add((String) data.get(key));
//                keys.append("`").append(fieldName).append("`");
//                values.append("\"").append("?").append("\"");
//                if (pos == data.keySet().size())
//                    break;
//                keys.append(",");
//                values.append(',');
//            }
//            query = query + keys.toString() + keyPart + values.toString() + valuePart;
//
//
//            pstmt = this.connection.prepareStatement(query);
//
//            pstmt.executeLargeUpdate(query);
////            stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
//            rs = stmt.getGeneratedKeys();
//        } catch (SQLException e) {
//            System.out.println("Occur some exception during insert data... message : " + e.getMessage());
//        } catch (ParseException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (stmt != null)
//                    stmt.close();
//                if (rs != null)
//                    rs.close();
//            } catch (SQLException e) {
//                System.out.println("Occur some exception during insert close... message : " + e.getMessage());
//            }
//        }
//    }

    public synchronized void insert(String tableName, String data) {
        String query;
        try (Statement stmt = this.connection.createStatement()) {
            query = this.makeInsertQuery(tableName, data);
            stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private String makeInsertQuery(String tableName, String row) throws ParseException {
        String queryStr = "INSERT INTO "+ tableName + "(";
        String keyPart = ") VALUES(";
        String valuePart = " )";

        int pos = 0;
        JSONObject data = (JSONObject) new JSONParser().parse(row);
        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (Object key : data.keySet()) {
            pos++;
            String fieldName = (String) key;
            String value = (String) data.get(key);
            keys.append("`").append(fieldName).append("`");
            if (this.dbType == DB_TYPE.MYSQL)
                values.append("\"").append(value).append("\"");
            else if (this.dbType == DB_TYPE.MARIA)
                values.append(value);
            if (pos == data.keySet().size())
                break;
            keys.append(",");
            values.append(',');
        }
        queryStr = queryStr + keys.toString() + keyPart + values.toString() + valuePart;

        return queryStr;
    }

    public void close() {
        try {
            if (this.connection != null)
                this.connection.close();
            System.out.println("※ DB connection close...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
