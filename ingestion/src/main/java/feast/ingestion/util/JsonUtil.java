package feast.ingestion.util;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;

public class JsonUtil {
  private static Gson gson = new Gson();
  /**
   * Unmarshals a given json string to map
   *
   * @param jsonString valid json formatted string
   * @return map of keys to values in json
   */
  public static Map<String, String> convertJsonStringToMap(String jsonString) {
    if (jsonString == null || jsonString.equals("") || jsonString.equals("{}")) {
      return Collections.emptyMap();
    }
    Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
    return gson.fromJson(jsonString, stringMapType);
  }
}
