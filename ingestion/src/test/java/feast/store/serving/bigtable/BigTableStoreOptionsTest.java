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

package feast.store.serving.bigtable;

import org.junit.Assert;
import org.junit.Test;
import feast.options.OptionsParser;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class BigTableStoreOptionsTest {

  @Test
  public void testParse() {
    BigTableStoreOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("project", "project1")
                .put("instance", "instance1")
                .put("prefix", "prefix_")
                .build(),
            BigTableStoreOptions.class);
    Assert.assertEquals("project1", options.project);
    Assert.assertEquals("instance1", options.instance);
    Assert.assertEquals("prefix_", options.prefix);
  }

  @Test
  public void testParseNoPrefix() {
    BigTableStoreOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("project", "project1")
                .put("instance", "instance1")
                .build(),
            BigTableStoreOptions.class);
    Assert.assertEquals("project1", options.project);
    Assert.assertEquals("instance1", options.instance);
    Assert.assertNull(options.prefix);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseNoProject() {
    OptionsParser.parse(
        ImmutableMap.<String, String>builder()
            .put("instance", "instance1")
            .put("prefix", "prefix_")
            .build(),
        BigTableStoreOptions.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseNoInstance() {
    OptionsParser.parse(
        ImmutableMap.<String, String>builder()
            .put("project", "project1")
            .put("prefix", "prefix_")
            .build(),
        BigTableStoreOptions.class);
  }
}
