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

import feast.ingestion.util.DateUtil;
import feast.storage.RedisProto.RedisBucketKey;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class FeatureRowToRedisMutationDoFnTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testRedisBucketKeySize() {
    Duration bucketSize = Duration.standardMinutes(20);
    DateTime dt = new DateTime(2018, 6, 22, 4, 15, 26);
    int bucketId =
        Math.toIntExact(DateUtil.toTimestamp(dt).getSeconds() / bucketSize.getStandardSeconds());
    String featureIdHash = DigestUtils.sha1Hex("driver.seconds.pings_v1").substring(0, 7);
    RedisBucketKey key =
        RedisBucketKey.newBuilder()
            .setEntityKey(String.valueOf(12345678))
            .setFeatureIdSha1Prefix(featureIdHash)
            .setBucketId(bucketId)
            .build();
    System.out.println(key.toByteArray().length);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutputMutationPerFeature() {
    // TODO
  }
}
