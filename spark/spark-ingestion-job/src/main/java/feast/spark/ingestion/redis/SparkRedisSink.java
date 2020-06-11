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
import feast.storage.connectors.redis.writer.RedisCustomIO;
import feast.storage.connectors.redis.writer.RedisCustomIO.Write;
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

/**
 * Sink for writing row data into Redis.
 *
 * <p>This sink does not use spark-redis as it replicates the custom serialization format of
 * feast-storage-connector-redis.
 */
public class SparkRedisSink implements SparkSink {

  private static final int DEFAULT_TIMEOUT = 2000;

  private final RedisConfig redisConfig;
  private final SparkSession spark;

  private final Map<String, FeatureSetSpec> featureSetSpecsByKey;

  public SparkRedisSink(
      RedisConfig redisConfig,
      SparkSession spark,
      Map<String, FeatureSetSpec> featureSetSpecsByKey) {
    this.redisConfig = redisConfig;
    this.spark = spark;
    this.featureSetSpecsByKey = featureSetSpecsByKey;
  }

  public VoidFunction2<Dataset<byte[]>, Long> configure() {

    Write write = new RedisCustomIO.Write(featureSetSpecsByKey);
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    Broadcast<Write> broadcastedWriter = sc.broadcast(write);

    return new RedisWriter(
        broadcastedWriter,
        new RedisURI(
            redisConfig.getHost(),
            redisConfig.getPort(),
            java.time.Duration.ofMillis(DEFAULT_TIMEOUT)));
  }

  @SuppressWarnings("serial")
  private static class RedisWriter implements VoidFunction2<Dataset<byte[]>, Long> {

    private final RedisURI uri;
    private final Broadcast<Write> broadcastedWriter;
    private transient RedisAsyncCommands<byte[], byte[]> commands = null;

    private RedisWriter(Broadcast<Write> broadcastedWriter, RedisURI uri) {
      this.uri = uri;
      this.broadcastedWriter = broadcastedWriter;
    }

    @Override
    public void call(Dataset<byte[]> v1, Long v2) throws Exception {

      Write writer = broadcastedWriter.getValue();
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
            byte[] key = writer.getKey(featureRow);
            if (key != null) {
              byte[] value = writer.getValue(featureRow);
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
