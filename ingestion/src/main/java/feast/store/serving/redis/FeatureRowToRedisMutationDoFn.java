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

package feast.store.serving.redis;

import feast.SerializableCache;
import feast.options.OptionsParser;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import feast.store.serving.redis.RedisCustomIO.Method;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Duration;

import java.util.Map;
import java.util.Random;

public class FeatureRowToRedisMutationDoFn extends DoFn<FeatureRowExtended, RedisMutation> {
  private Random random;
  private Map<String, FeatureSpec> featureSpecByFeatureId;

  private final SerializableCache<FeatureSpec, RedisFeatureOptions> servingOptionsCache =
      SerializableCache.<FeatureSpec, RedisFeatureOptions>builder()
          .loadingFunction(
              (featureSpec) ->
                  OptionsParser.lenientParse(
                      featureSpec.getOptionsMap(), RedisFeatureOptions.class))
          .build();

  public FeatureRowToRedisMutationDoFn(Map<String, FeatureSpec> featureSpecByFeatureId) {
    this.random = new Random();
    this.featureSpecByFeatureId = featureSpecByFeatureId;
  }

  static RedisBucketKey getRedisBucketKey(
      String entityId, String featureIdSha1Prefix, long bucketId) {
    return RedisBucketKey.newBuilder()
        .setEntityKey(entityId)
        .setFeatureIdSha1Prefix(featureIdSha1Prefix)
        .setBucketId(bucketId)
        .build();
  }

  static String getFeatureIdSha1Prefix(String featureId) {
    return DigestUtils.sha1Hex(featureId.getBytes()).substring(0, 7);
  }

  /** Output a redis mutation object for every feature in the feature row. */
  @ProcessElement
  public void processElement(ProcessContext context) {
    FeatureRow fetureRow = context.element().getRow();
    String entityKey = fetureRow.getEntityKey();

    for (Feature feature : fetureRow.getFeaturesList()) {
      String featureId = feature.getId();
      if (!featureSpecByFeatureId.containsKey(featureId)) {
        continue;
      }

      FeatureSpec featureSpec = featureSpecByFeatureId.get(featureId);
      String featureIdHash = getFeatureIdSha1Prefix(featureId);

      RedisFeatureOptions options = servingOptionsCache.get(featureSpec);

      RedisBucketValue value =
          RedisBucketValue.newBuilder()
              .setValue(feature.getValue())
              .setEventTimestamp(fetureRow.getEventTimestamp())
              .build();
      RedisBucketKey keyForLatest = getRedisBucketKey(entityKey, featureIdHash, 0L);

      Duration expiry = options.getExpiryDuration();
      // Add randomness to expiry so that it won't expire in the same time.
      long expiryMillis = (long) (expiry.getMillis() * (1 + random.nextFloat()));
      context.output(
          RedisMutation.builder()
              .key(keyForLatest.toByteArray())
              .value(value.toByteArray())
              .expiryMillis(expiryMillis)
              .method(Method.SET)
              .build());
    }
  }
}
