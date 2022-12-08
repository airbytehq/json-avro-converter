package tech.allegro.schema.json2avro.converter.util;

import java.util.Arrays;

public class NumberUtil {

  private static String[] INFINITY_NAN = {"infinity", "-infinity", "nan"};

  public static boolean isInfinityOrNan(String value) {
    return Arrays.stream(INFINITY_NAN).anyMatch(value::equalsIgnoreCase);
  }

}
