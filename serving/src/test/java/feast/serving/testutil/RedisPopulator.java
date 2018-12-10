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

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.util.SpecUtil;
import feast.serving.util.TimeUtil;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import feast.types.GranularityProto.Granularity.Enum;
import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Collection;

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
      Timestamp start,
      Timestamp end) {
    for (FeatureSpec fs : featureSpecs) {
      for (String entityId : entityIds) {
        if (fs.getGranularity().equals(Enum.NONE)) {
          addGranularityNoneData(entityId, fs);
        } else {
          start = TimeUtil.roundFloorTimestamp(start, fs.getGranularity());
          end = TimeUtil.roundFloorTimestamp(end, fs.getGranularity());
          addOtherGranularityData(entityId, fs, start, end);
        }
      }
    }
  }

  /**
   * Add time series data for entityId within specified time range.
   *
   * @param entityId entity id of the data to be added.
   * @param fs feature spec of the feature to be added.
   * @param start start time of the available data (exclusive).
   * @param end end time of the available data (inclusive).
   */
  private void addOtherGranularityData(
      String entityId, FeatureSpec fs, Timestamp start, Timestamp end) {
    Duration dur = getTimestep(fs.getGranularity());
    long bucketSizeInSecond = SpecUtil.getBucketSize(fs).getStandardSeconds();
    String featureId = fs.getId();
    String featureIdSha1Prefix = getFeatureIdSha1Prefix(featureId);
    Pipeline pipeline = jedis.pipelined();

    for (Timestamp iter = end;
        Timestamps.compare(iter, start) > 0;
        iter = Timestamps.subtract(iter, dur)) {
      long bucketId =
          TimeUtil.roundFloorTimestamp(iter, fs.getGranularity()).getSeconds() / bucketSizeInSecond;
      RedisBucketKey bucketKeyProto = createBucketKey(entityId, featureIdSha1Prefix, bucketId);
      RedisBucketKey keyProto = createBucketKey(entityId, featureIdSha1Prefix, iter.getSeconds());

      RedisBucketValue valueProto =
          RedisBucketValue.newBuilder()
              .setValue(createValue(entityId, featureId, iter, fs.getValueType()))
              .setEventTimestamp(iter)
              .build();

      byte[] bucketKey = bucketKeyProto.toByteArray();
      byte[] key = keyProto.toByteArray();
      byte[] value = valueProto.toByteArray();
      pipeline.set(key, value);
      pipeline.zadd(bucketKey, iter.getSeconds(), key);

      // add last value in bucket 0
      if (Timestamps.compare(iter, end) == 0) {
        RedisBucketKey lastValueKey = createBucketKey(entityId, featureIdSha1Prefix, 0);
        pipeline.set(lastValueKey.toByteArray(), value);
      }
    }
    pipeline.sync();
  }

  /**
   * Add singular data as key value.
   *
   * @param entityId entityId of data to be added.
   * @param fs feature spec of the feature to be added.
   */
  private void addGranularityNoneData(String entityId, FeatureSpec fs) {
    RedisBucketKey bucketKey = createBucketKey(entityId, getFeatureIdSha1Prefix(fs.getId()), 0);
    RedisBucketValue bucketValue =
        RedisBucketValue.newBuilder().setValue(createValue(entityId, fs.getId(), fs.getValueType())).build();
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
