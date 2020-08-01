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
package feast.storage.connectors.redis.writer;

import static feast.storage.common.testing.TestUtil.field;
import static feast.storage.common.testing.TestUtil.hash;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.RedisClusterConfig;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType.Enum;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.ByteArrayCodec;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.ishiis.redis.unit.Redis;
import net.ishiis.redis.unit.RedisCluster;
import net.ishiis.redis.unit.RedisServer;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RedisFeatureSinkTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private static String REDIS_HOST = "localhost";
  private static int REDIS_PORT = 51233;
  private static Integer[] REDIS_CLUSTER_PORTS = {6380, 6381, 6382};

  private RedisStringCommands<byte[], byte[]> sync;
  private RedisFeatureSink redisFeatureSink;
  private Map<FeatureSetReference, FeatureSetSpec> specMap;

  @Parameterized.Parameters
  public static Iterable<Object[]> backends() {
    Redis redis = new RedisServer(REDIS_PORT);
    RedisClient client =
        RedisClient.create(new RedisURI(REDIS_HOST, REDIS_PORT, java.time.Duration.ofMillis(2000)));

    Redis redisCluster = new RedisCluster(REDIS_CLUSTER_PORTS);
    RedisClusterClient clientCluster =
        RedisClusterClient.create(
            Lists.newArrayList(REDIS_CLUSTER_PORTS).stream()
                .map(port -> RedisURI.create(REDIS_HOST, port))
                .collect(Collectors.toList()));

    StoreProto.Store.RedisConfig redisConfig =
        StoreProto.Store.RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();

    StoreProto.Store.RedisClusterConfig redisClusterConfig =
        StoreProto.Store.RedisClusterConfig.newBuilder()
            .setConnectionString(
                Lists.newArrayList(REDIS_CLUSTER_PORTS).stream()
                    .map(port -> String.format("%s:%d", REDIS_HOST, port))
                    .collect(Collectors.joining(",")))
            .setInitialBackoffMs(2000)
            .setMaxRetries(4)
            .build();

    return Arrays.asList(
        new Object[] {redis, client, redisConfig},
        new Object[] {redisCluster, clientCluster, redisClusterConfig});
  }

  @Parameterized.Parameter(0)
  public Redis redisServer;

  @Parameterized.Parameter(1)
  public AbstractRedisClient redisClient;

  @Parameterized.Parameter(2)
  public Message redisConfig;

  @Before
  public void setUp() {
    redisServer.start();

    if (redisClient instanceof RedisClient) {
      sync = ((RedisClient) redisClient).connect(new ByteArrayCodec()).sync();
    } else {
      sync = ((RedisClusterClient) redisClient).connect(new ByteArrayCodec()).sync();
    }

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

    specMap =
        ImmutableMap.of(
            FeatureSetReference.of("myproject", "fs", 1), spec1,
            FeatureSetReference.of("myproject", "feature_set", 1), spec2);

    RedisFeatureSink.Builder builder = RedisFeatureSink.builder();
    if (redisConfig instanceof RedisConfig) {
      builder = builder.setRedisConfig((RedisConfig) redisConfig);
    } else {
      builder = builder.setRedisClusterConfig((RedisClusterConfig) redisConfig);
    }
    redisFeatureSink = builder.build();
    redisFeatureSink.prepareWrite(p.apply("Specs-1", Create.of(specMap)));
  }

  @After
  public void tearDown() {
    if (redisServer.isActive()) {
      redisServer.stop();
    }
  }

  private RedisKey createRedisKey(String featureSetRef, Field... fields) {
    return RedisKey.newBuilder()
        .setFeatureSet(featureSetRef)
        .addAllEntities(Lists.newArrayList(fields))
        .build();
  }

  private FeatureRow createFeatureRow(String featureSetRef, Timestamp timestamp, Field... fields) {
    FeatureRow.Builder builder = FeatureRow.newBuilder();
    if (featureSetRef != null) {
      builder.setFeatureSet(featureSetRef);
    }

    if (timestamp != null) {
      builder.setEventTimestamp(timestamp);
    }

    return builder.addAllFields(Lists.newArrayList(fields)).build();
  }

  @Test
  public void shouldWriteToRedis() {

    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        createRedisKey("myproject/fs", field("entity", 1, Enum.INT64)),
        createFeatureRow(
            null, Timestamp.getDefaultInstance(), field(hash("feature"), "one", Enum.STRING)));
    kvs.put(
        createRedisKey("myproject/fs", field("entity", 2, Enum.INT64)),
        createFeatureRow(
            null, Timestamp.getDefaultInstance(), field(hash("feature"), "two", Enum.STRING)));

    List<FeatureRow> featureRows =
        ImmutableList.of(
            createFeatureRow(
                "myproject/fs",
                null,
                field("entity", 1, Enum.INT64),
                field("feature", "one", Enum.STRING)),
            createFeatureRow(
                "myproject/fs",
                null,
                field("entity", 2, Enum.INT64),
                field("feature", "two", Enum.STRING)));

    p.apply(Create.of(featureRows)).apply(redisFeatureSink.writer());
    p.run();

    kvs.forEach(
        (key, value) -> {
          byte[] actual = sync.get(key.toByteArray());
          assertThat(actual, equalTo(value.toByteArray()));
        });
  }

  @Test(timeout = 30000)
  public void shouldRetryFailConnection() throws InterruptedException {
    RedisConfig redisConfig =
        RedisConfig.newBuilder()
            .setHost(REDIS_HOST)
            .setPort(REDIS_PORT)
            .setMaxRetries(4)
            .setInitialBackoffMs(2000)
            .build();
    redisFeatureSink =
        redisFeatureSink
            .toBuilder()
            .setRedisConfig(redisConfig)
            .build()
            .withSpecsView(redisFeatureSink.getSpecsView());

    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        createRedisKey("myproject/fs", field("entity", 1, Enum.INT64)),
        createFeatureRow(
            "", Timestamp.getDefaultInstance(), field(hash("feature"), "one", Enum.STRING)));

    List<FeatureRow> featureRows =
        ImmutableList.of(
            createFeatureRow(
                "myproject/fs",
                null,
                field("entity", 1, Enum.INT64),
                field("feature", "one", Enum.STRING)));

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRows))
            .apply(redisFeatureSink.writer())
            .getFailedInserts()
            .apply(Count.globally());

    redisServer.stop();
    final ScheduledThreadPoolExecutor redisRestartExecutor = new ScheduledThreadPoolExecutor(1);
    ScheduledFuture<?> scheduledRedisRestart =
        redisRestartExecutor.schedule(
            () -> {
              redisServer.start();
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
    redisFeatureSink =
        redisFeatureSink
            .toBuilder()
            .setRedisConfig(redisConfig)
            .build()
            .withSpecsView(redisFeatureSink.getSpecsView());

    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        createRedisKey("myproject/fs", field("entity", 1, Enum.INT64)),
        createFeatureRow(
            "", Timestamp.getDefaultInstance(), field(hash("feature"), "one", Enum.STRING)));

    List<FeatureRow> featureRows =
        ImmutableList.of(
            FeatureRow.newBuilder()
                .setFeatureSet("myproject/fs")
                .addFields(field("entity", 1, Enum.INT64))
                .addFields(field("feature", "one", Enum.STRING))
                .build());

    PCollection<Long> failedElementCount =
        p.apply(Create.of(featureRows))
            .apply(redisFeatureSink.writer())
            .getFailedInserts()
            .apply(Count.globally());

    redisServer.stop();
    PAssert.that(failedElementCount).containsInAnyOrder(1L);
    p.run();
  }

  @Test
  public void shouldConvertRowWithDuplicateEntitiesToValidKey() {

    FeatureRow offendingRow =
        createFeatureRow(
            "myproject/feature_set",
            Timestamp.newBuilder().setSeconds(10).build(),
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_primary", 2, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING),
            field("feature_1", "strValue1", Enum.STRING),
            field("feature_2", 1001, Enum.INT64));

    RedisKey expectedKey =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING));

    FeatureRow expectedValue =
        createFeatureRow(
            "",
            Timestamp.newBuilder().setSeconds(10).build(),
            field(hash("feature_1"), "strValue1", Enum.STRING),
            field(hash("feature_2"), 1001, Enum.INT64));

    p.apply(Create.of(offendingRow)).apply(redisFeatureSink.writer());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldConvertRowWithOutOfOrderFieldsToValidKey() {
    FeatureRow offendingRow =
        createFeatureRow(
            "myproject/feature_set",
            Timestamp.newBuilder().setSeconds(10).build(),
            field("entity_id_secondary", "a", Enum.STRING),
            field("entity_id_primary", 1, Enum.INT32),
            field("feature_2", 1001, Enum.INT64),
            field("feature_1", "strValue1", Enum.STRING));

    RedisKey expectedKey =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING));

    FeatureRow expectedValue =
        createFeatureRow(
            "",
            Timestamp.newBuilder().setSeconds(10).build(),
            field(hash("feature_1"), "strValue1", Enum.STRING),
            field(hash("feature_2"), 1001, Enum.INT64));

    p.apply(Create.of(offendingRow)).apply(redisFeatureSink.writer());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldMergeDuplicateFeatureFields() {
    FeatureRow featureRowWithDuplicatedFeatureFields =
        createFeatureRow(
            "myproject/feature_set",
            Timestamp.newBuilder().setSeconds(10).build(),
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING),
            field("feature_2", 1001, Enum.INT64),
            field("feature_1", "strValue1", Enum.STRING),
            field("feature_1", "strValue1", Enum.STRING));

    RedisKey expectedKey =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING));

    FeatureRow expectedValue =
        createFeatureRow(
            "",
            Timestamp.newBuilder().setSeconds(10).build(),
            field(hash("feature_1"), "strValue1", Enum.STRING),
            field(hash("feature_2"), 1001, Enum.INT64));

    p.apply(Create.of(featureRowWithDuplicatedFeatureFields)).apply(redisFeatureSink.writer());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldPopulateMissingFeatureValuesWithDefaultInstance() {
    FeatureRow featureRowWithDuplicatedFeatureFields =
        createFeatureRow(
            "myproject/feature_set",
            Timestamp.newBuilder().setSeconds(10).build(),
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING),
            field("feature_1", "strValue1", Enum.STRING));

    RedisKey expectedKey =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING));

    FeatureRow expectedValue =
        createFeatureRow(
            "",
            Timestamp.newBuilder().setSeconds(10).build(),
            field(hash("feature_1"), "strValue1", Enum.STRING),
            Field.newBuilder()
                .setName(hash("feature_2"))
                .setValue(Value.getDefaultInstance())
                .build());

    p.apply(Create.of(featureRowWithDuplicatedFeatureFields)).apply(redisFeatureSink.writer());

    p.run();

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldDeduplicateRowsWithinBatch() {
    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .addElements(
                createFeatureRow(
                    "myproject/feature_set",
                    Timestamp.newBuilder().setSeconds(20).build(),
                    field("entity_id_primary", 1, Enum.INT32),
                    field("entity_id_secondary", "a", Enum.STRING),
                    field("feature_2", 111, Enum.INT32)))
            .addElements(
                createFeatureRow(
                    "myproject/feature_set",
                    Timestamp.newBuilder().setSeconds(10).build(),
                    field("entity_id_primary", 1, Enum.INT32),
                    field("entity_id_secondary", "a", Enum.STRING),
                    field("feature_2", 222, Enum.INT32)))
            .addElements(
                createFeatureRow(
                    "myproject/feature_set",
                    Timestamp.getDefaultInstance(),
                    field("entity_id_primary", 1, Enum.INT32),
                    field("entity_id_secondary", "a", Enum.STRING),
                    field("feature_2", 333, Enum.INT32)))
            .advanceWatermarkToInfinity();

    p.apply(featureRowTestStream).apply(redisFeatureSink.writer());
    p.run();

    RedisKey expectedKey =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING));

    FeatureRow expectedValue =
        createFeatureRow(
            "",
            Timestamp.newBuilder().setSeconds(20).build(),
            Field.newBuilder()
                .setName(hash("feature_1"))
                .setValue(Value.getDefaultInstance())
                .build(),
            field(hash("feature_2"), 111, Enum.INT32));

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void shouldWriteWithLatterTimestamp() {
    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .addElements(
                createFeatureRow(
                    "myproject/feature_set",
                    Timestamp.newBuilder().setSeconds(20).build(),
                    field("entity_id_primary", 1, Enum.INT32),
                    field("entity_id_secondary", "a", Enum.STRING),
                    field("feature_2", 111, Enum.INT32)))
            .addElements(
                createFeatureRow(
                    "myproject/feature_set",
                    Timestamp.newBuilder().setSeconds(20).build(),
                    field("entity_id_primary", 2, Enum.INT32),
                    field("entity_id_secondary", "b", Enum.STRING),
                    field("feature_2", 222, Enum.INT32)))
            .addElements(
                createFeatureRow(
                    "myproject/feature_set",
                    Timestamp.newBuilder().setSeconds(10).build(),
                    field("entity_id_primary", 3, Enum.INT32),
                    field("entity_id_secondary", "c", Enum.STRING),
                    field("feature_2", 333, Enum.INT32)))
            .advanceWatermarkToInfinity();

    RedisKey keyA =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING));

    RedisKey keyB =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 2, Enum.INT32),
            field("entity_id_secondary", "b", Enum.STRING));

    RedisKey keyC =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 3, Enum.INT32),
            field("entity_id_secondary", "c", Enum.STRING));

    sync.set(
        keyA.toByteArray(),
        createFeatureRow("", Timestamp.newBuilder().setSeconds(30).build()).toByteArray());

    sync.set(
        keyB.toByteArray(),
        createFeatureRow("", Timestamp.newBuilder().setSeconds(10).build()).toByteArray());

    sync.set(
        keyC.toByteArray(),
        createFeatureRow("", Timestamp.newBuilder().setSeconds(10).build()).toByteArray());

    p.apply(featureRowTestStream).apply(redisFeatureSink.writer());
    p.run();

    assertThat(
        sync.get(keyA.toByteArray()),
        equalTo(createFeatureRow("", Timestamp.newBuilder().setSeconds(30).build()).toByteArray()));

    assertThat(
        sync.get(keyB.toByteArray()),
        equalTo(
            createFeatureRow(
                    "",
                    Timestamp.newBuilder().setSeconds(20).build(),
                    Field.newBuilder()
                        .setName(hash("feature_1"))
                        .setValue(Value.getDefaultInstance())
                        .build(),
                    field(hash("feature_2"), 222, Enum.INT32))
                .toByteArray()));

    assertThat(
        sync.get(keyC.toByteArray()),
        equalTo(createFeatureRow("", Timestamp.newBuilder().setSeconds(10).build()).toByteArray()));
  }

  @Test
  public void shouldOverwriteInvalidRows() {
    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .addElements(
                createFeatureRow(
                    "myproject/feature_set",
                    Timestamp.newBuilder().setSeconds(20).build(),
                    field("entity_id_primary", 1, Enum.INT32),
                    field("entity_id_secondary", "a", Enum.STRING),
                    field("feature_1", "text", Enum.STRING),
                    field("feature_2", 111, Enum.INT32)))
            .advanceWatermarkToInfinity();

    RedisKey expectedKey =
        createRedisKey(
            "myproject/feature_set",
            field("entity_id_primary", 1, Enum.INT32),
            field("entity_id_secondary", "a", Enum.STRING));

    sync.set(expectedKey.toByteArray(), "some-invalid-data".getBytes());

    p.apply(featureRowTestStream).apply(redisFeatureSink.writer());
    p.run();

    FeatureRow expectedValue =
        createFeatureRow(
            "",
            Timestamp.newBuilder().setSeconds(20).build(),
            field(hash("feature_1"), "text", Enum.STRING),
            field(hash("feature_2"), 111, Enum.INT32));

    byte[] actual = sync.get(expectedKey.toByteArray());
    assertThat(actual, equalTo(expectedValue.toByteArray()));
  }

  @Test
  public void loadTest() {
    List<FeatureRow> rows =
        IntStream.range(0, 10000)
            .mapToObj(
                i ->
                    createFeatureRow(
                        "myproject/feature_set",
                        Timestamp.newBuilder().setSeconds(20).build(),
                        field("entity_id_primary", i, Enum.INT32),
                        field("entity_id_secondary", "a", Enum.STRING),
                        field("feature_1", "text", Enum.STRING),
                        field("feature_2", 111, Enum.INT32)))
            .collect(Collectors.toList());

    p.apply(Create.of(rows)).apply(redisFeatureSink.writer());
    p.run();

    List<byte[]> outcome =
        IntStream.range(0, 10000)
            .mapToObj(
                i ->
                    createRedisKey(
                            "myproject/feature_set",
                            field("entity_id_primary", i, Enum.INT32),
                            field("entity_id_secondary", "a", Enum.STRING))
                        .toByteArray())
            .map(sync::get)
            .collect(Collectors.toList());

    assertThat(outcome, hasSize(10000));
    assertThat("All rows were stored", outcome.stream().allMatch(Objects::nonNull));
  }
}
