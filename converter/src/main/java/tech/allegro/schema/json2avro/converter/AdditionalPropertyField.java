package tech.allegro.schema.json2avro.converter;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

public class AdditionalPropertyField {

  public static final String FIELD_NAME = "_ab_additional_properties";
  public static final Schema FIELD_SCHEMA = Schema.createUnion(
      Schema.create(Schema.Type.NULL),
      Schema.createMap(Schema.create(Type.STRING)));
  public static final Field FIELD = new Field(FIELD_NAME, FIELD_SCHEMA, null, null);

  public static Map<String, String> getMapValue(Map<String, Object> genericValue) {
    return genericValue.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        r -> {
          JsonNode jsonNode = JsonHelper.jsonNode(r.getValue());
          if (jsonNode.isTextual()) {
            return jsonNode.asText();
          } else {
            return JsonHelper.serialize(jsonNode);
          }
        }));
  }

  public static String getValue(Object genericValue) {
    JsonNode jsonNode = JsonHelper.jsonNode(genericValue);
    if (jsonNode.isTextual()) {
      return jsonNode.asText();
    } else {
      return JsonHelper.serialize(jsonNode);
    }
  }

}
