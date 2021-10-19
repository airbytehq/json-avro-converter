package tech.allegro.schema.json2avro.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class JsonAvroConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  public static <T> String serialize(final T object) {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static JsonNode deserialize(final String jsonString) {
    try {
      return MAPPER.readTree(jsonString);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  public static String readResource(final String name) throws IOException {
    final URL resource = Resources.getResource(name);
    return Resources.toString(resource, StandardCharsets.UTF_8);
  }

  public static <T> List<T> toList(final Iterator<T> iterator) {
    final List<T> list = new ArrayList<>();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return list;
  }

  public static class JsonToAvroConverterTestCaseProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
      final JsonNode testCases = deserialize(readResource("json_avro_converter.json"));
      return toList(testCases.elements()).stream().map(testCase -> {
        System.out.println(testCase);
        return Arguments.of(
            testCase.get("testCase").asText(),
            testCase.get("avroSchema"),
            testCase.get("jsonObject"),
            testCase.get("avroObject"));
      });
    }
  }

  @ParameterizedTest
  @ArgumentsSource(JsonToAvroConverterTestCaseProvider.class)
  public void testJsonToAvroConverter(String testCaseName, JsonNode avroSchema, JsonNode jsonObject, JsonNode avroObject)
      throws JsonProcessingException {
    JsonAvroConverter converter = JsonAvroConverter.builder().build();
    Schema schema =  new Schema.Parser().parse(serialize(avroSchema));
    GenericData.Record actualAvroObject = converter.convertToGenericDataRecord(WRITER.writeValueAsBytes(jsonObject), schema);
    assertEquals(avroObject, deserialize(actualAvroObject.toString()), String.format("Test for %s failed", testCaseName));
  }

}
