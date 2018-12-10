/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.storage.redis;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import feast.options.OptionsParser;

public class RedisStoreOptionsTest {

  @Test
  public void testParse() {
    Map<String, String> map = new HashMap<>();
    map.put("host", "localhost");
    map.put("port", "1234");

    RedisStoreOptions opts = OptionsParser.parse(map, RedisStoreOptions.class);
    assertEquals("localhost", opts.host);
    assertEquals(1234, (int) opts.port);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseHost() {
    Map<String, String> map = new HashMap<>();
    map.put("port", "1234");
    OptionsParser.parse(map, RedisStoreOptions.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseNoPort() {
    Map<String, String> map = new HashMap<>();
    map.put("host", "localhost");
    OptionsParser.parse(map, RedisStoreOptions.class);
  }
}
