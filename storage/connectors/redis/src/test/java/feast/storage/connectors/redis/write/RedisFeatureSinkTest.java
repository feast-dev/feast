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
package feast.storage.connectors.redis.write;

import static feast.storage.common.testing.TestUtil.field;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.embedded.Redis;
import redis.embedded.RedisServer;

public class RedisFeatureSinkTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private static String REDIS_HOST = "localhost";
  private static int REDIS_PORT = 51234;
  private Redis redis;
  private RedisClient redisClient;
  private RedisStringCommands<byte[], byte[]> sync;

  private RedisFeatureSink redisFeatureSink;

  @Before
  public void setUp() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
    redisClient =
        RedisClient.create(new RedisURI(REDIS_HOST, REDIS_PORT, java.time.Duration.ofMillis(2000)));
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    sync = connection.sync();

    FeatureSetSpec spec1 =
        FeatureSetSpec.newBuilder()
            .setName("fs")
            .setVersion(1)
            .setProject("myproject")
            .addEntities(EntitySpec.newBuilder().setName("entity").setValueType(Enum.INT64).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature").setValueType(Enum.STRING).build())
            .build();

    FeatureSetSpec spec2 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
            .setVersion(1)
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_1").setValueType(Enum.STRING).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_2").setValueType(Enum.INT64).build())
            .build();

    Map<String, FeatureSetSpec> specMap =
        ImmutableMap.of("myproject/fs:1", spec1, "myproject/feature_set:1", spec2);
    StoreProto.Store.RedisConfig redisConfig =
        StoreProto.Store.RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();

    redisFeatureSink =
        RedisFeatureSink.builder().setFeatureSetSpecs(specMap).setRedisConfig(redisConfig).build();
  }

  @After
  public void teardown() {
    redisClient.shutdown();
    redis.stop();
  }

  @Test
  public void shouldWriteToRedis() {

    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("myproject/fs:1")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one")))
            .build());
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("myproject/fs:1")
            .addEntities(field("entity", 2, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("two")))
            .build());

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs:1")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs:1")
                .addFields(field("entity", 2, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .build());

    p.apply(Create.of(featureRows)).apply(redisFeatureSink.write());
    p.run();

    kvs.forEach(
        (key, value) -> {
          byte[] actual = sync.get(key.toByteArray());
          assertThat(actual, equalTo(value.toByteArray()));
        });
  }

  @Test(timeout = 10000)
  public void shouldRetryFailConnection() throws InterruptedException {
    RedisConfig redisConfig =
        RedisConfig.newBuilder()
            .setHost(REDIS_HOST)
            .setPort(REDIS_PORT)
            .setMaxRetries(4)
            .setInitialBackoffMs(2000)
            .build();
    redisFeatureSink = redisFeatureSink.toBuilder().setRedisConfig(redisConfig).build();

    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("myproject/fs:1")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one")))
            .build());

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs:1")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build());

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRows))
            .apply(redisFeatureSink.write())
            .getFailedInserts()
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
          byte[] actual = sync.get(key.toByteArray());
          assertThat(actual, equalTo(value.toByteArray()));
        });
  }

  @Test
  public void shouldProduceFailedElementIfRetryExceeded() {

    RedisConfig redisConfig =
        RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT + 1).build();
    redisFeatureSink = redisFeatureSink.toBuilder().setRedisConfig(redisConfig).build();

    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("myproject/fs:1")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one")))
            .build());

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs:1")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build());

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRows))
            .apply(redisFeatureSink.write())
            .getFailedInserts()
            .apply(Count.globally());

    redis.stop();
    PAssert.that(failedElementCount).containsInAnyOrder(1L);
    p.run();
  }

  @Test
  public void shouldConvertRowWithDuplicateEntitiesToValidKey() {

    FeatureRow offendingRow =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(2)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_2")
                    .setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    p.apply(Create.of(offendingRow)).apply(redisFeatureSink.write());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldConvertRowWithOutOfOrderFieldsToValidKey() {
    FeatureRow offendingRow =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("feature_2")
                    .setValue(Value.newBuilder().setInt64Val(1001)))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .build();

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    List<Field> expectedFields =
        Arrays.asList(
            Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")).build(),
            Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001)).build());
    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addAllFields(expectedFields)
            .build();

    p.apply(Create.of(offendingRow)).apply(redisFeatureSink.write());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldMergeDuplicateFeatureFields() {
    FeatureRow featureRowWithDuplicatedFeatureFields =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_2")
                    .setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    p.apply(Create.of(featureRowWithDuplicatedFeatureFields)).apply(redisFeatureSink.write());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldPopulateMissingFeatureValuesWithDefaultInstance() {
    FeatureRow featureRowWithDuplicatedFeatureFields =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .build();

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("myproject/feature_set:1")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(Field.newBuilder().setValue(Value.getDefaultInstance()))
            .build();

    p.apply(Create.of(featureRowWithDuplicatedFeatureFields)).apply(redisFeatureSink.write());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }
}
