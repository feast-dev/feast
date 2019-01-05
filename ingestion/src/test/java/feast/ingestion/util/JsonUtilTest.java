package feast.ingestion.util;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class JsonUtilTest {
  @Test
  public void convertJsonStringToMapShouldConvertJsonStringToMap() {
    String input = "{\"key\": \"value\"}";
    Map<String, String> expected = new HashMap<>();
    expected.put("key", "value");
    assertThat(JsonUtil.convertJsonStringToMap(input), equalTo(expected));
  }

  @Test
  public void convertJsonStringToMapShouldReturnEmptyMapForEmptyJson() {
    String input = "{}";
    Map<String, String> expected = Collections.emptyMap();
    assertThat(JsonUtil.convertJsonStringToMap(input), equalTo(expected));
  }
}