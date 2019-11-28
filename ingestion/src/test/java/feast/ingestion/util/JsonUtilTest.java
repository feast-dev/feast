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
package feast.ingestion.util;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import feast.ingestion.utils.JsonUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

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
