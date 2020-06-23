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
package feast.storage.connectors.redis.writer;

import com.google.auto.value.AutoValue;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisClusterConfig;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

@AutoValue
public abstract class RedisFeatureSink implements FeatureSink {

  /**
   * Initialize a {@link RedisFeatureSink.Builder} from a {@link StoreProto.Store.RedisConfig}.
   *
   * @param redisConfig {@link RedisConfig}
   * @return {@link RedisFeatureSink.Builder}
   */
  public static FeatureSink fromConfig(RedisConfig redisConfig) {
    return builder().setRedisConfig(redisConfig).build();
  }

  public static FeatureSink fromConfig(RedisClusterConfig redisConfig) {
    return builder().setRedisClusterConfig(redisConfig).build();
  }

  @Nullable
  public abstract RedisConfig getRedisConfig();

  @Nullable
  public abstract RedisClusterConfig getRedisClusterConfig();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_RedisFeatureSink.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setRedisConfig(RedisConfig redisConfig);

    public abstract Builder setRedisClusterConfig(RedisClusterConfig redisConfig);

    public abstract RedisFeatureSink build();
  }

  PCollectionView<Map<String, Iterable<FeatureSetSpec>>> specsView;

  public RedisFeatureSink withSpecsView(
      PCollectionView<Map<String, Iterable<FeatureSetSpec>>> specsView) {
    this.specsView = specsView;
    return this;
  }

  PCollectionView<Map<String, Iterable<FeatureSetSpec>>> getSpecsView() {
    return specsView;
  }

  @Override
  public PCollection<FeatureSetReference> prepareWrite(
      PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs) {
    if (getRedisConfig() != null) {
      RedisClient redisClient =
          RedisClient.create(
              RedisURI.create(getRedisConfig().getHost(), getRedisConfig().getPort()));
      try {
        redisClient.connect();
      } catch (RedisConnectionException e) {
        throw new RuntimeException(
            String.format(
                "Failed to connect to Redis at host: '%s' port: '%d'. Please check that your Redis is running and accessible from Feast.",
                getRedisConfig().getHost(), getRedisConfig().getPort()));
      }
      redisClient.shutdown();
    } else if (getRedisClusterConfig() == null) {
      throw new RuntimeException(
          "At least one RedisConfig or RedisClusterConfig must be provided to Redis Sink");
    }
    specsView = featureSetSpecs.apply(ParDo.of(new ReferenceToString())).apply(View.asMultimap());
    return featureSetSpecs.apply(Keys.create());
  }

  @Override
  public PTransform<PCollection<FeatureRow>, WriteResult> writer() {
    if (getRedisClusterConfig() != null) {
      return new RedisCustomIO.Write(
          new RedisClusterIngestionClient(getRedisClusterConfig()), getSpecsView());
    } else if (getRedisConfig() != null) {
      return new RedisCustomIO.Write(
          new RedisStandaloneIngestionClient(getRedisConfig()), getSpecsView());
    } else {
      throw new RuntimeException(
          "At least one RedisConfig or RedisClusterConfig must be provided to Redis Sink");
    }
  }

  private static class ReferenceToString
      extends DoFn<KV<FeatureSetReference, FeatureSetSpec>, KV<String, FeatureSetSpec>> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(KV.of(c.element().getKey().getReference(), c.element().getValue()));
    }
  }
}
