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
package feast.spark.ingestion.redis;

import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.SparkSink;
import feast.storage.connectors.redis.writer.RedisCustomIO.Write.WriteDoFn;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;

/**
 * Sink for writing row data into Redis.
 *
 * <p>This sink does not use spark-redis as it replicates the custom serialization format of
 * feast-storage-connector-redis.
 */
public class SparkRedisSink implements SparkSink {

  private static final int DEFAULT_TIMEOUT = 2000;

  private final RedisConfig redisConfig;
  private final Map<String, FeatureSetSpec> featureSetSpecsByKey;

  public SparkRedisSink(RedisConfig redisConfig, Map<String, FeatureSetSpec> featureSetSpecsByKey) {
    this.redisConfig = redisConfig;
    this.featureSetSpecsByKey = featureSetSpecsByKey;
  }

  public VoidFunction2<Dataset<byte[]>, Long> configure() {

    RedisURI redisuri =
        new RedisURI(
            redisConfig.getHost(),
            redisConfig.getPort(),
            java.time.Duration.ofMillis(DEFAULT_TIMEOUT));

    String password = redisConfig.getPass();
    if (StringUtils.trimToNull(password) != null) {
      redisuri.setPassword(password);
    }

    return new RedisWriter(redisuri, featureSetSpecsByKey);
  }

  @SuppressWarnings("serial")
  private static class RedisWriter implements VoidFunction2<Dataset<byte[]>, Long> {

    private final RedisURI uri;
    private final Map<String, FeatureSetSpec> featureSetSpecsByKey;
    private transient RedisAsyncCommands<byte[], byte[]> commands = null;

    private RedisWriter(RedisURI uri, Map<String, FeatureSetSpec> featureSetSpecsByKey) {
      this.uri = uri;
      this.featureSetSpecsByKey = featureSetSpecsByKey;
    }

    @Override
    public void call(Dataset<byte[]> v1, Long v2) throws Exception {

      List<RedisFuture<?>> futures = new ArrayList<>();
      v1.foreach(
          r -> {
            if (commands == null) {
              RedisClient redisclient = RedisClient.create(uri);
              StatefulRedisConnection<byte[], byte[]> connection =
                  redisclient.connect(new ByteArrayCodec());
              commands = connection.async();
            }
            FeatureRow featureRow = FeatureRow.parseFrom(r);
            FeatureSetSpec spec = featureSetSpecsByKey.get(featureRow.getFeatureSet());
            byte[] key = WriteDoFn.getKey(featureRow, spec);
            if (key != null) {
              byte[] value = WriteDoFn.getValue(featureRow, spec);
              futures.add(commands.set(key, value));
            }
          });
      try {
        LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture<?>[0]));
      } finally {
        futures.clear();
      }
    }
  }
}
