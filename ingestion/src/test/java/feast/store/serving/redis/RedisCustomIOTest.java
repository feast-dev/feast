/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.store.serving.redis;

import static feast.test.TestUtil.field;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import feast.core.StoreProto;
import feast.storage.RedisProto.RedisKey;
import feast.store.serving.redis.RedisCustomIO.Method;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.ValueType.Enum;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.Redis;
import redis.embedded.RedisServer;

public class RedisCustomIOTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private static String REDIS_HOST = "localhost";
  private static int REDIS_PORT = 51234;
  private Redis redis;
  private Jedis jedis;

  @Before
  public void setUp() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
    jedis = new Jedis(REDIS_HOST, REDIS_PORT);
  }

  @After
  public void teardown() {
    redis.stop();
  }

  @Test
  public void shouldWriteToRedis() {
    StoreProto.Store.RedisConfig redisConfig =
        StoreProto.Store.RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();
    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("fs:1")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setFeatureSet("fs:1")
            .addFields(field("entity", 1, Enum.INT64))
            .addFields(field("feature", "one", Enum.STRING))
            .build());
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("fs:1")
            .addEntities(field("entity", 2, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setFeatureSet("fs:1")
            .addFields(field("entity", 2, Enum.INT64))
            .addFields(field("feature", "two", Enum.STRING))
            .build());

    List<RedisMutation> featureRowWrites =
        kvs.entrySet().stream()
            .map(
                kv ->
                    new RedisMutation(
                        Method.SET,
                        kv.getKey().toByteArray(),
                        kv.getValue().toByteArray(),
                        null,
                        null))
            .collect(Collectors.toList());

    p.apply(Create.of(featureRowWrites)).apply(RedisCustomIO.write(redisConfig));
    p.run();

    kvs.forEach(
        (key, value) -> {
          byte[] actual = jedis.get(key.toByteArray());
          assertThat(actual, equalTo(value.toByteArray()));
        });
  }

  @Test(timeout = 10000)
  public void shouldRetryFailConnection() throws InterruptedException {
    StoreProto.Store.RedisConfig redisConfig =
        StoreProto.Store.RedisConfig.newBuilder()
            .setHost(REDIS_HOST)
            .setPort(REDIS_PORT)
            .setMaxRetries(4)
            .setInitialBackoffMs(2000)
            .build();
    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("fs:1")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setFeatureSet("fs:1")
            .addFields(field("entity", 1, Enum.INT64))
            .addFields(field("feature", "one", Enum.STRING))
            .build());

    List<RedisMutation> featureRowWrites =
        kvs.entrySet().stream()
            .map(
                kv ->
                    new RedisMutation(
                        Method.SET,
                        kv.getKey().toByteArray(),
                        kv.getValue().toByteArray(),
                        null,
                        null))
            .collect(Collectors.toList());

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRowWrites))
            .apply(RedisCustomIO.write(redisConfig))
            .apply(Count.globally());

    redis.stop();
    final ScheduledThreadPoolExecutor redisRestartExecutor = new ScheduledThreadPoolExecutor(1);
    ScheduledFuture<?> scheduledRedisRestart =
        redisRestartExecutor.schedule(
            () -> {
              redis.start();
            },
            3,
            TimeUnit.SECONDS);

    PAssert.that(failedElementCount).containsInAnyOrder(0L);
    p.run();
    scheduledRedisRestart.cancel(true);

    kvs.forEach(
        (key, value) -> {
          byte[] actual = jedis.get(key.toByteArray());
          assertThat(actual, equalTo(value.toByteArray()));
        });
  }

  @Test
  public void shouldProduceFailedElementIfRetryExceeded() {
    StoreProto.Store.RedisConfig redisConfig =
        StoreProto.Store.RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();
    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("fs:1")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setFeatureSet("fs:1")
            .addFields(field("entity", 1, Enum.INT64))
            .addFields(field("feature", "one", Enum.STRING))
            .build());

    List<RedisMutation> featureRowWrites =
        kvs.entrySet().stream()
            .map(
                kv ->
                    new RedisMutation(
                        Method.SET,
                        kv.getKey().toByteArray(),
                        kv.getValue().toByteArray(),
                        null,
                        null))
            .collect(Collectors.toList());

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRowWrites))
            .apply(RedisCustomIO.write(redisConfig))
            .apply(Count.globally());

    redis.stop();
    PAssert.that(failedElementCount).containsInAnyOrder(1L);
    p.run();
  }
}
