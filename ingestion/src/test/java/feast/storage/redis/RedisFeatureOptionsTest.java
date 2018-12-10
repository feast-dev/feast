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

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import feast.options.OptionsParser;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class RedisFeatureOptionsTest {

  @Test
  public void testParse() {
    RedisFeatureOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("bucketSize", "PT3H")
                .put("expiry", "PT6H")
                .build(),
            RedisFeatureOptions.class);
    Assert.assertEquals("PT3H", options.bucketSize);
    Assert.assertEquals("PT6H", options.expiry);

    Assert.assertEquals(Duration.standardHours(3), options.getBucketSizeDuration());
    Assert.assertEquals(Duration.standardHours(6), options.getExpiryDuration());
  }

  @Test
  public void testParseNoBucketSize() {
    RedisFeatureOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("expiry", "PT6H")
                .build(),
            RedisFeatureOptions.class);
    Assert.assertEquals(RedisFeatureOptions.DEFAULT_BUCKET_SIZE, options.bucketSize);
    Assert.assertEquals("PT6H", options.expiry);

    Assert.assertEquals(Duration.standardHours(1), options.getBucketSizeDuration());
    Assert.assertEquals(Duration.standardHours(6), options.getExpiryDuration());
  }


  @Test
  public void testParseNoExpiry() {
    RedisFeatureOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("bucketSize", "PT3H")
                .build(),
            RedisFeatureOptions.class);
    Assert.assertEquals(RedisFeatureOptions.DEFAULT_EXPIRY, options.expiry);
    Assert.assertEquals("PT3H", options.bucketSize);

    Assert.assertEquals(Duration.ZERO, options.getExpiryDuration());
    Assert.assertEquals(Duration.standardHours(3), options.getBucketSizeDuration());
  }
}
