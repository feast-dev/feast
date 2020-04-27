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
package feast.storage.connectors.rediscluster.writer;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto;
import feast.core.StoreProto.Store.RedisClusterConfig;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import feast.types.FeatureRowProto;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class RedisClusterFeatureSink implements FeatureSink {

  /**
   * Initialize a {@link RedisClusterFeatureSink.Builder} from a {@link RedisClusterConfig}.
   *
   * @param redisClusterConfig {@link RedisClusterConfig}
   * @param featureSetSpecs
   * @return {@link RedisClusterFeatureSink.Builder}
   */
  public static FeatureSink fromConfig(
      RedisClusterConfig redisClusterConfig,
      Map<String, FeatureSetProto.FeatureSetSpec> featureSetSpecs) {
    return builder()
        .setFeatureSetSpecs(featureSetSpecs)
        .setRedisClusterConfig(redisClusterConfig)
        .build();
  }

  public abstract RedisClusterConfig getRedisClusterConfig();

  public abstract Map<String, FeatureSetProto.FeatureSetSpec> getFeatureSetSpecs();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_RedisClusterFeatureSink.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRedisClusterConfig(RedisClusterConfig redisClusterConfig);

    public abstract Builder setFeatureSetSpecs(
        Map<String, FeatureSetProto.FeatureSetSpec> featureSetSpecs);

    public abstract RedisClusterFeatureSink build();
  }

  @Override
  public void prepareWrite(FeatureSetProto.FeatureSet featureSet) {}

  @Override
  public PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> writer() {
    return new RedisClusterCustomIO.Write(getRedisClusterConfig(), getFeatureSetSpecs());
  }
}
