/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.storage.connectors.redis.write;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store.RedisConfig;
import feast.storage.api.write.FeatureSink;
import feast.storage.api.write.WriteResult;
import feast.types.FeatureRowProto.FeatureRow;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class RedisFeatureSink implements FeatureSink {

  public abstract RedisConfig getRedisConfig();

  public abstract Map<String, FeatureSetSpec> getFeatureSetSpecs();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_RedisFeatureSink.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRedisConfig(RedisConfig redisConfig);

    public abstract Builder setFeatureSetSpecs(Map<String, FeatureSetSpec> featureSetSpecs);

    public abstract RedisFeatureSink build();
  }

  @Override
  public void prepareWrite(FeatureSet featureSet) {
    RedisClient redisClient =
        RedisClient.create(RedisURI.create(getRedisConfig().getHost(), getRedisConfig().getPort()));
    try {
      redisClient.connect();
    } catch (RedisConnectionException e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to Redis at host: '%s' port: '%d'. Please check that your Redis is running and accessible from Feast.",
              getRedisConfig().getHost(), getRedisConfig().getPort()));
    }
    redisClient.shutdown();
  }

  @Override
  public PTransform<PCollection<FeatureRow>, WriteResult> write() {
    return new RedisCustomIO.Write(getRedisConfig(), getFeatureSetSpecs());
  }
}
