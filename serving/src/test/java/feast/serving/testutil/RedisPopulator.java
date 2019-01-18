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

package feast.serving.testutil;

import com.google.protobuf.Timestamp;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import java.util.Collection;
import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.Jedis;

public class RedisPopulator extends FeatureStoragePopulator {
  private final Jedis jedis;

  public RedisPopulator(String redisHost, int redisPort) {
    jedis = new Jedis(redisHost, redisPort);
  }

  @Override
  public void populate(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      Timestamp timestamp) {
    for (FeatureSpec fs : featureSpecs) {
      for (String entityId : entityIds) {
        Timestamp roundedTimestamp = TimeUtil.roundFloorTimestamp(timestamp, fs.getGranularity());
        addData(entityId, fs, roundedTimestamp);
      }
    }
  }

  /**
   * Add feature value.
   *
   * @param entityId entityId of data to be added.
   * @param fs feature spec of the feature to be added.
   * @param timestamp timestamp of data
   */
  private void addData(String entityId, FeatureSpec fs, Timestamp timestamp) {
    RedisBucketKey bucketKey = createBucketKey(entityId, getFeatureIdSha1Prefix(fs.getId()), 0);
    RedisBucketValue bucketValue =
        RedisBucketValue.newBuilder()
            .setValue(createValue(entityId, fs.getId(), timestamp, fs.getValueType()))
            .setEventTimestamp(timestamp)
            .build();
    byte[] key = bucketKey.toByteArray();
    byte[] value = bucketValue.toByteArray();
    jedis.set(key, value);
  }

  /**
   * Create {@link RedisBucketKey}.
   *
   * @param entityId
   * @param featureIdSha1Prefix
   * @param bucketId
   * @return
   */
  private RedisBucketKey createBucketKey(
      String entityId, String featureIdSha1Prefix, long bucketId) {
    return RedisBucketKey.newBuilder()
        .setEntityKey(entityId)
        .setFeatureIdSha1Prefix(featureIdSha1Prefix)
        .setBucketId(bucketId)
        .build();
  }

  /**
   * Convenient function to calculate feature id's sha1 prefix.
   *
   * @param featureId feature ID
   * @return first 7 characters of SHA1(featureID)
   */
  private String getFeatureIdSha1Prefix(String featureId) {
    return DigestUtils.sha1Hex(featureId.getBytes()).substring(0, 7);
  }
}
