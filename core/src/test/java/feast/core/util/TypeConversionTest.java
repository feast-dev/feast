/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.core.util;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

import com.google.protobuf.Timestamp;
import java.util.*;
import org.junit.Test;

public class TypeConversionTest {
  @Test
  public void convertTimeStampShouldCorrectlyConvertDateToProtobufTimestamp() {
    Date date = new Date(1000);
    Timestamp expected = Timestamp.newBuilder().setSeconds(1).build();
    assertThat(TypeConversion.convertTimestamp(date), equalTo(expected));
  }

  @Test
  public void convertTagStringToListShouldConvertTagStringToList() {
    String input = "value1,value2";
    List<String> expected = Arrays.asList("value1", "value2");
    assertThat(TypeConversion.convertTagStringToList(input), equalTo(expected));
  }

  @Test
  public void convertTagStringToListShouldReturnEmptyListForEmptyString() {
    String input = "";
    List<String> expected = Collections.emptyList();
    assertThat(TypeConversion.convertTagStringToList(input), equalTo(expected));
  }

  @Test
  public void convertJsonStringToMapShouldConvertJsonStringToMap() {
    String input = "{\"key\": \"value\"}";
    Map<String, String> expected = new HashMap<>();
    expected.put("key", "value");
    assertThat(TypeConversion.convertJsonStringToMap(input), equalTo(expected));
  }

  @Test
  public void convertJsonStringToMapShouldReturnEmptyMapForEmptyJson() {
    String input = "{}";
    Map<String, String> expected = Collections.emptyMap();
    assertThat(TypeConversion.convertJsonStringToMap(input), equalTo(expected));
  }

  @Test
  public void convertMapToJsonStringShouldReturnJsonStringForGivenMap() {
    Map<String, String> input = new HashMap<>();
    input.put("key", "value");
    assertThat(
        TypeConversion.convertMapToJsonString(input), hasJsonPath("$.key", equalTo("value")));
  }

  @Test
  public void convertMapToJsonStringShouldReturnEmptyJsonForAnEmptyMap() {
    Map<String, String> input = new HashMap<>();
    assertThat(TypeConversion.convertMapToJsonString(input), equalTo("{}"));
  }

  @Test
  public void convertJsonStringToArgsShouldReturnCorrectListOfArgs() {
    Map<String, String> input = new HashMap<>();
    input.put("key", "value");
    input.put("key2", "value2");

    String[] expected = new String[] {"--key=value", "--key2=value2"};
    String[] actual = TypeConversion.convertMapToArgs(input);
    assertThat(actual.length, equalTo(expected.length));
    assertTrue(Arrays.asList(actual).containsAll(Arrays.asList(expected)));
  }
}
