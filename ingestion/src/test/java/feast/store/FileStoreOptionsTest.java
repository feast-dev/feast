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

package feast.store;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.Test;
import feast.options.OptionsParser;

public class FileStoreOptionsTest {

  @Test
  public void testOptions() {
    FileStoreOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder().put("path", "/tmp/asdg").build(),
            FileStoreOptions.class);
    assertEquals("/tmp/asdg", options.path);
    assertEquals(FileStoreOptions.DEFAULT_WINDOW_SIZE, options.getWindowDuration());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOptionsInvalidWindowSize() {
    FileStoreOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("path", "/tmp/asdg")
                .put("windowSize", "dsaf")
                .build(),
            FileStoreOptions.class);
  }

  @Test
  public void testOptionsValidWindowSize() {
    FileStoreOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("path", "/tmp/asdg")
                .put("windowSize", "PT1H")
                .build(),
            FileStoreOptions.class);
    assertEquals("/tmp/asdg", options.path);
    assertEquals(Duration.standardHours(1), options.getWindowDuration());
  }
}
