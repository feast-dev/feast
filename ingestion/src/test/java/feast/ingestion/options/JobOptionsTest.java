/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.ingestion.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import feast.options.OptionsParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JobOptionsTest {

  @Test
  public void test_shouldParseAll() {
    Map<String, String> map = new HashMap<>();
    map.put("coalesceRows.enabled", "false");
    map.put("coalesceRows.delaySeconds", "123");
    map.put("coalesceRows.timeoutSeconds", "1800");
    map.put("sample.limit", "1234");


    JobOptions options = OptionsParser.parse(map, JobOptions.class);
    assertEquals(options.getSampleLimit(), 1234L);
    assertEquals(options.getCoalesceRowsDelaySeconds(), 123L);
    assertFalse(options.isCoalesceRowsEnabled());
    assertEquals(options.getCoalesceRowsTimeoutSeconds(), 1800L);
  }

  @Test
  public void test_shouldParseEmptyOptions() {
    JobOptions options = OptionsParser.parse(new HashMap<>(), JobOptions.class);
    assertEquals(options.getSampleLimit(), 0L);
    assertEquals(options.getCoalesceRowsDelaySeconds(), 0L);
    assertTrue(options.isCoalesceRowsEnabled()); //defaults to true
    assertEquals(options.getCoalesceRowsTimeoutSeconds(), 0L);
  }

  @Test
  public void test_shouldParseSampleLimit() {
    Map<String, String> map = new HashMap<>();
    map.put("sample.limit", "1234");
    JobOptions options = OptionsParser.parse(map, JobOptions.class);
    assertEquals(options.getSampleLimit(), 1234L);
  }

  @Test
  public void test_shouldParseCoalesceRowsDelaySeconds() {
    Map<String, String> map = new HashMap<>();
    map.put("coalesceRows.delaySeconds", "123");
    JobOptions options = OptionsParser.parse(map, JobOptions.class);
    assertEquals(options.getCoalesceRowsDelaySeconds(), 123L);
  }

  @Test
  public void test_shouldParseCoalesceRowsEnabled() {
    Map<String, String> map = new HashMap<>();
    map.put("coalesceRows.enabled", "true");
    JobOptions options = OptionsParser.parse(map, JobOptions.class);
    assertTrue(options.isCoalesceRowsEnabled());

  }


  @Test
  public void test_shouldParseCoalesceRowsTimeoutSeconds() {
    Map<String, String> map = new HashMap<>();
    map.put("coalesceRows.timeoutSeconds", "1800");
    JobOptions options = OptionsParser.parse(map, JobOptions.class);
    assertEquals(options.getCoalesceRowsTimeoutSeconds(), 1800L);
  }
}