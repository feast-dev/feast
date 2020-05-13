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
package feast.storage.connectors.rediscluster.writer;

import static feast.storage.common.testing.TestUtil.field;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store.RedisClusterConfig;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import net.ishiis.redis.unit.RedisCluster;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class RedisClusterFeatureSinkTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private static String REDIS_CLUSTER_HOST = "localhost";
  private static int REDIS_CLUSTER_PORT1 = 6380;
  private static int REDIS_CLUSTER_PORT2 = 6381;
  private static int REDIS_CLUSTER_PORT3 = 6382;
  private static String CONNECTION_STRING = "localhost:6380,localhost:6381,localhost:6382";
  private RedisCluster redisCluster;
  private RedisClusterClient redisClusterClient;
  private RedisClusterCommands<byte[], byte[]> redisClusterCommands;

  private RedisClusterFeatureSink redisClusterFeatureSink;

  @Before
  public void setUp() throws IOException {
    redisCluster = new RedisCluster(REDIS_CLUSTER_PORT1, REDIS_CLUSTER_PORT2, REDIS_CLUSTER_PORT3);
    redisCluster.start();
    redisClusterClient =
        RedisClusterClient.create(
            Arrays.asList(
                RedisURI.create(REDIS_CLUSTER_HOST, REDIS_CLUSTER_PORT1),
                RedisURI.create(REDIS_CLUSTER_HOST, REDIS_CLUSTER_PORT2),
                RedisURI.create(REDIS_CLUSTER_HOST, REDIS_CLUSTER_PORT3)));
    StatefulRedisClusterConnection<byte[], byte[]> connection =
        redisClusterClient.connect(new ByteArrayCodec());
    redisClusterCommands = connection.sync();
    redisClusterCommands.setTimeout(java.time.Duration.ofMillis(600000));

    FeatureSetSpec spec1 =
        FeatureSetSpec.newBuilder()
            .setName("fs")
            .setProject("myproject")
            .addEntities(EntitySpec.newBuilder().setName("entity").setValueType(Enum.INT64).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature").setValueType(Enum.STRING).build())
            .build();

    FeatureSetSpec spec2 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
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
        ImmutableMap.of("myproject/fs", spec1, "myproject/feature_set", spec2);
    RedisClusterConfig redisClusterConfig =
        RedisClusterConfig.newBuilder()
            .setConnectionString(CONNECTION_STRING)
            .setInitialBackoffMs(2000)
            .setMaxRetries(4)
            .build();

    redisClusterFeatureSink =
        RedisClusterFeatureSink.builder()
            .setFeatureSetSpecs(specMap)
            .setRedisClusterConfig(redisClusterConfig)
            .build();
  }

  static boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  @After
  public void teardown() {
    redisClusterClient.shutdown();
    redisCluster.stop();
    deleteDirectory(new File(String.valueOf(Paths.get(System.getProperty("user.dir"), ".redis"))));
  }

  @Test
  public void shouldWriteToRedis() {

    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("myproject/fs")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one")))
            .build());
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("myproject/fs")
            .addEntities(field("entity", 2, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("two")))
            .build());

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build(),
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs")
                .addFields(field("entity", 2, Enum.INT64))
                .addFields(field("feature", "two", Enum.STRING))
                .build());

    p.apply(Create.of(featureRows)).apply(redisClusterFeatureSink.writer());
    p.run();

    kvs.forEach(
        (key, value) -> {
          byte[] actual = redisClusterCommands.get(key.toByteArray());
          assertThat(actual, equalTo(value.toByteArray()));
        });
  }

  @Test(timeout = 15000)
  public void shouldRetryFailConnection() throws InterruptedException {
    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        RedisKey.newBuilder()
            .setFeatureSet("myproject/fs")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one")))
            .build());

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build());

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRows))
            .apply(redisClusterFeatureSink.writer())
            .getFailedInserts()
            .apply(Count.globally());

    redisCluster.stop();
    final ScheduledThreadPoolExecutor redisRestartExecutor = new ScheduledThreadPoolExecutor(1);
    ScheduledFuture<?> scheduledRedisRestart =
        redisRestartExecutor.schedule(
            () -> {
              redisCluster.start();
            },
            3,
            TimeUnit.SECONDS);

    PAssert.that(failedElementCount).containsInAnyOrder(0L);
    p.run();
    scheduledRedisRestart.cancel(true);

    kvs.forEach(
        (key, value) -> {
          byte[] actual = redisClusterCommands.get(key.toByteArray());
          assertThat(actual, equalTo(value.toByteArray()));
        });
  }

  @Test
  public void shouldProduceFailedElementIfRetryExceeded() {
    RedisClusterConfig redisClusterConfig =
        RedisClusterConfig.newBuilder()
            .setConnectionString(CONNECTION_STRING)
            .setInitialBackoffMs(2000)
            .setMaxRetries(1)
            .build();

    FeatureSetSpec spec1 =
        FeatureSetSpec.newBuilder()
            .setName("fs")
            .setProject("myproject")
            .addEntities(EntitySpec.newBuilder().setName("entity").setValueType(Enum.INT64).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature").setValueType(Enum.STRING).build())
            .build();
    Map<String, FeatureSetSpec> specMap = ImmutableMap.of("myproject/fs", spec1);
    redisClusterFeatureSink =
        RedisClusterFeatureSink.builder()
            .setFeatureSetSpecs(specMap)
            .setRedisClusterConfig(redisClusterConfig)
            .build();
    redisCluster.stop();

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build());

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRows))
            .apply(redisClusterFeatureSink.writer())
            .getFailedInserts()
            .apply(Count.globally());

    PAssert.that(failedElementCount).containsInAnyOrder(1L);
    p.run();
  }

  @Test
  public void shouldConvertRowWithDuplicateEntitiesToValidKey() {

    FeatureRow offendingRow =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set")
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
            .setFeatureSet("myproject/feature_set")
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

    p.apply(Create.of(offendingRow)).apply(redisClusterFeatureSink.writer());

    p.run();

    byte[] actual = redisClusterCommands.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldConvertRowWithOutOfOrderFieldsToValidKey() {
    FeatureRow offendingRow =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set")
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
            .setFeatureSet("myproject/feature_set")
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

    p.apply(Create.of(offendingRow)).apply(redisClusterFeatureSink.writer());

    p.run();

    byte[] actual = redisClusterCommands.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldMergeDuplicateFeatureFields() {
    FeatureRow featureRowWithDuplicatedFeatureFields =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set")
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
            .setFeatureSet("myproject/feature_set")
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

    p.apply(Create.of(featureRowWithDuplicatedFeatureFields))
        .apply(redisClusterFeatureSink.writer());

    p.run();

    byte[] actual = redisClusterCommands.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldPopulateMissingFeatureValuesWithDefaultInstance() {
    FeatureRow featureRowWithDuplicatedFeatureFields =
        FeatureRow.newBuilder()
            .setFeatureSet("myproject/feature_set")
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
            .setFeatureSet("myproject/feature_set")
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

    p.apply(Create.of(featureRowWithDuplicatedFeatureFields))
        .apply(redisClusterFeatureSink.writer());

    p.run();

    byte[] actual = redisClusterCommands.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }
}
