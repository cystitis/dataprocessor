package min.test.dataprocessor.config;

import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Avro schema 정보를 관리하는 class.
 */
public class AvroSchema {
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String FIELDS = "fields";
    public static final String FIELD_TYPE_INTEGER = "int";
    public static final String FIELD_TYPE_LONG = "long";
    public static final String FIELD_TYPE_DOUBLE = "double";
    public static final String FIELD_TYPE_STRING = "string";
    public static final String FIELD_TYPE_TIME_STAMP = "timestamp";
    private final String schemaName;
    private final Schema schema;

    /**
     * schema.json 파일의 정보로 avro schema 를 생성한다.
     * @param schemaName schema name
     * @param fieldObj field info of schema
     */
    AvroSchema(String schemaName, JSONObject fieldObj) {
        this.schemaName = schemaName;
        Schema.Parser parser = new Schema.Parser();
        this.schema = parser.parse(this.makeAvroSchema(fieldObj).toJSONString());
    }

    @SuppressWarnings("unchecked")
    private JSONObject makeAvroSchema(JSONObject fieldObj) {
        JSONObject format = new JSONObject();
        format.put(NAME, this.schemaName);
        format.put(TYPE, "record");
        JSONArray fieldArr = new JSONArray();
        for (Object key : fieldObj.keySet()) {
            JSONObject inner = new JSONObject();
            String fieldName = (String) key;
            String fieldType = (String) fieldObj.get(key);
            if ("integer".equals(fieldType)) fieldType = "int";
            inner.put(NAME, fieldName);
            inner.put(TYPE, fieldType);
            fieldArr.add(inner);
        }
        format.put(FIELDS, fieldArr);

        return format;
    }

    public String getSchemaName() { return this.schemaName; }

    public Schema getSchema() { return this.schema; }
}
