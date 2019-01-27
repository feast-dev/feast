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

public class BigTableFeatureOptionsTest {

  @Test
  public void testParse() {
    BigTableFeatureOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder().put("family", "family1").build(),
            BigTableFeatureOptions.class);
    Assert.assertEquals("family1", options.family);
  }

  @Test
  public void testParseNoFamily() {
    BigTableFeatureOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder().build(),
            BigTableFeatureOptions.class);
    Assert.assertEquals(BigTableFeatureOptions.DEFAULT_FAMILY, options.family);
  }
}
