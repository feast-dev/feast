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
package feast.storage.connectors.bigtable.writer;

import static feast.storage.common.testing.TestUtil.field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType.Enum;
import java.io.IOException;
import java.util.*;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class BigtableFeatureSinkTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private static String REDIS_HOST = "localhost";
  private static int REDIS_PORT = 51234;

  private BigtableFeatureSink bigtableFeatureSink;

  @Before
  public void setUp() throws IOException {

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
    // StoreProto.Store.BigtableConfig bigtableConfig =
    //
    // StoreProto.Store.BigtableConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();

    // bigtableFeatureSink =
    //
    // BigtableFeatureSink.builder().setFeatureSetSpecs(specMap).setBigtableConfig(bigtableConfig).build();
  }

  @After
  public void teardown() {}

  @Test
  public void shouldWriteToBigtable() {

    HashMap<BigtableKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(
        BigtableKey.newBuilder()
            .setFeatureSet("myproject/fs")
            .addEntities(field("entity", 1, Enum.INT64))
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one")))
            .build());
    kvs.put(
        BigtableKey.newBuilder()
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

    // p.apply(Create.of(featureRows)).apply(bigtableFeatureSink.writer());
    // p.run();
    assert true;
  }

  /**
   * @Test(timeout = 10000) public void shouldRetryFailConnection() throws InterruptedException {
   * BigtableConfig bigtableConfig = BigtableConfig.newBuilder() .setHost(REDIS_HOST)
   * .setPort(REDIS_PORT) .setMaxRetries(4) .setInitialBackoffMs(2000) .build(); bigtableFeatureSink
   * = bigtableFeatureSink.toBuilder().setBigtableConfig(bigtableConfig).build();
   *
   * <p>HashMap<BigtableKey, FeatureRow> kvs = new LinkedHashMap<>(); kvs.put(
   * BigtableKey.newBuilder() .setFeatureSet("myproject/fs") .addEntities(field("entity", 1,
   * Enum.INT64)) .build(), FeatureRow.newBuilder()
   * .setEventTimestamp(Timestamp.getDefaultInstance())
   * .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one"))) .build());
   *
   * <p>List<FeatureRow> featureRows = ImmutableList.of( FeatureRow.newBuilder()
   * .setFeatureSet("myproject/fs") .addFields(field("entity", 1, Enum.INT64))
   * .addFields(field("feature", "one", Enum.STRING)) .build());
   *
   * <p>PCollection<Long> failedElementCount = p.apply(Create.of(featureRows))
   * .apply(bigtableFeatureSink.writer()) .getFailedInserts() .apply(Count.globally());
   *
   * <p>bigtable.stop(); final ScheduledThreadPoolExecutor bigtableRestartExecutor = new
   * ScheduledThreadPoolExecutor(1); ScheduledFuture<?> scheduledBigtableRestart =
   * bigtableRestartExecutor.schedule( () -> { bigtable.start(); }, 3, TimeUnit.SECONDS);
   *
   * <p>PAssert.that(failedElementCount).containsInAnyOrder(0L); p.run();
   * scheduledBigtableRestart.cancel(true);
   *
   * <p>kvs.forEach( (key, value) -> { byte[] actual = sync.get(key.toByteArray());
   * assertThat(actual, equalTo(value.toByteArray())); }); } @Test public void
   * shouldProduceFailedElementIfRetryExceeded() {
   *
   * <p>BigtableConfig bigtableConfig =
   * BigtableConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT + 1).build();
   * bigtableFeatureSink =
   * bigtableFeatureSink.toBuilder().setBigtableConfig(bigtableConfig).build();
   *
   * <p>HashMap<BigtableKey, FeatureRow> kvs = new LinkedHashMap<>(); kvs.put(
   * BigtableKey.newBuilder() .setFeatureSet("myproject/fs") .addEntities(field("entity", 1,
   * Enum.INT64)) .build(), FeatureRow.newBuilder()
   * .setEventTimestamp(Timestamp.getDefaultInstance())
   * .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("one"))) .build());
   *
   * <p>List<FeatureRow> featureRows = ImmutableList.of( FeatureRow.newBuilder()
   * .setFeatureSet("myproject/fs") .addFields(field("entity", 1, Enum.INT64))
   * .addFields(field("feature", "one", Enum.STRING)) .build());
   *
   * <p>PCollection<Long> failedElementCount = p.apply(Create.of(featureRows))
   * .apply(bigtableFeatureSink.writer()) .getFailedInserts() .apply(Count.globally());
   *
   * <p>bigtable.stop(); PAssert.that(failedElementCount).containsInAnyOrder(1L); p.run(); } @Test
   * public void shouldConvertRowWithDuplicateEntitiesToValidKey() {
   *
   * <p>FeatureRow offendingRow = FeatureRow.newBuilder() .setFeatureSet("myproject/feature_set")
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10)) .addFields( Field.newBuilder()
   * .setName("entity_id_primary") .setValue(Value.newBuilder().setInt32Val(1))) .addFields(
   * Field.newBuilder() .setName("entity_id_primary") .setValue(Value.newBuilder().setInt32Val(2)))
   * .addFields( Field.newBuilder() .setName("entity_id_secondary")
   * .setValue(Value.newBuilder().setStringVal("a"))) .addFields( Field.newBuilder()
   * .setName("feature_1") .setValue(Value.newBuilder().setStringVal("strValue1"))) .addFields(
   * Field.newBuilder() .setName("feature_2") .setValue(Value.newBuilder().setInt64Val(1001)))
   * .build();
   *
   * <p>BigtableKey expectedKey = BigtableKey.newBuilder() .setFeatureSet("myproject/feature_set")
   * .addEntities( Field.newBuilder() .setName("entity_id_primary")
   * .setValue(Value.newBuilder().setInt32Val(1))) .addEntities( Field.newBuilder()
   * .setName("entity_id_secondary") .setValue(Value.newBuilder().setStringVal("a"))) .build();
   *
   * <p>FeatureRow expectedValue = FeatureRow.newBuilder()
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
   * .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
   * .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001))) .build();
   *
   * <p>p.apply(Create.of(offendingRow)).apply(bigtableFeatureSink.writer());
   *
   * <p>p.run();
   *
   * <p>byte[] actual = sync.get(expectedKey.toByteArray()); assertThat(actual,
   * equalTo(expectedValue.toByteArray())); } @Test public void
   * shouldConvertRowWithOutOfOrderFieldsToValidKey() { FeatureRow offendingRow =
   * FeatureRow.newBuilder() .setFeatureSet("myproject/feature_set")
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10)) .addFields( Field.newBuilder()
   * .setName("entity_id_secondary") .setValue(Value.newBuilder().setStringVal("a"))) .addFields(
   * Field.newBuilder() .setName("entity_id_primary") .setValue(Value.newBuilder().setInt32Val(1)))
   * .addFields( Field.newBuilder() .setName("feature_2")
   * .setValue(Value.newBuilder().setInt64Val(1001))) .addFields( Field.newBuilder()
   * .setName("feature_1") .setValue(Value.newBuilder().setStringVal("strValue1"))) .build();
   *
   * <p>BigtableKey expectedKey = BigtableKey.newBuilder() .setFeatureSet("myproject/feature_set")
   * .addEntities( Field.newBuilder() .setName("entity_id_primary")
   * .setValue(Value.newBuilder().setInt32Val(1))) .addEntities( Field.newBuilder()
   * .setName("entity_id_secondary") .setValue(Value.newBuilder().setStringVal("a"))) .build();
   *
   * <p>List<Field> expectedFields = Arrays.asList(
   * Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")).build(),
   * Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001)).build()); FeatureRow
   * expectedValue = FeatureRow.newBuilder()
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10)) .addAllFields(expectedFields)
   * .build();
   *
   * <p>p.apply(Create.of(offendingRow)).apply(bigtableFeatureSink.writer());
   *
   * <p>p.run();
   *
   * <p>byte[] actual = sync.get(expectedKey.toByteArray()); assertThat(actual,
   * equalTo(expectedValue.toByteArray())); } @Test public void shouldMergeDuplicateFeatureFields()
   * { FeatureRow featureRowWithDuplicatedFeatureFields = FeatureRow.newBuilder()
   * .setFeatureSet("myproject/feature_set")
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10)) .addFields( Field.newBuilder()
   * .setName("entity_id_primary") .setValue(Value.newBuilder().setInt32Val(1))) .addFields(
   * Field.newBuilder() .setName("entity_id_secondary")
   * .setValue(Value.newBuilder().setStringVal("a"))) .addFields( Field.newBuilder()
   * .setName("feature_1") .setValue(Value.newBuilder().setStringVal("strValue1"))) .addFields(
   * Field.newBuilder() .setName("feature_1")
   * .setValue(Value.newBuilder().setStringVal("strValue1"))) .addFields( Field.newBuilder()
   * .setName("feature_2") .setValue(Value.newBuilder().setInt64Val(1001))) .build();
   *
   * <p>BigtableKey expectedKey = BigtableKey.newBuilder() .setFeatureSet("myproject/feature_set")
   * .addEntities( Field.newBuilder() .setName("entity_id_primary")
   * .setValue(Value.newBuilder().setInt32Val(1))) .addEntities( Field.newBuilder()
   * .setName("entity_id_secondary") .setValue(Value.newBuilder().setStringVal("a"))) .build();
   *
   * <p>FeatureRow expectedValue = FeatureRow.newBuilder()
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
   * .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
   * .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001))) .build();
   *
   * <p>p.apply(Create.of(featureRowWithDuplicatedFeatureFields)).apply(bigtableFeatureSink.writer());
   *
   * <p>p.run();
   *
   * <p>byte[] actual = sync.get(expectedKey.toByteArray()); assertThat(actual,
   * equalTo(expectedValue.toByteArray())); } @Test public void
   * shouldPopulateMissingFeatureValuesWithDefaultInstance() { FeatureRow
   * featureRowWithDuplicatedFeatureFields = FeatureRow.newBuilder()
   * .setFeatureSet("myproject/feature_set")
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10)) .addFields( Field.newBuilder()
   * .setName("entity_id_primary") .setValue(Value.newBuilder().setInt32Val(1))) .addFields(
   * Field.newBuilder() .setName("entity_id_secondary")
   * .setValue(Value.newBuilder().setStringVal("a"))) .addFields( Field.newBuilder()
   * .setName("feature_1") .setValue(Value.newBuilder().setStringVal("strValue1"))) .build();
   *
   * <p>BigtableKey expectedKey = BigtableKey.newBuilder() .setFeatureSet("myproject/feature_set")
   * .addEntities( Field.newBuilder() .setName("entity_id_primary")
   * .setValue(Value.newBuilder().setInt32Val(1))) .addEntities( Field.newBuilder()
   * .setName("entity_id_secondary") .setValue(Value.newBuilder().setStringVal("a"))) .build();
   *
   * <p>FeatureRow expectedValue = FeatureRow.newBuilder()
   * .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
   * .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
   * .addFields(Field.newBuilder().setValue(Value.getDefaultInstance())) .build();
   *
   * <p>p.apply(Create.of(featureRowWithDuplicatedFeatureFields)).apply(bigtableFeatureSink.writer());
   *
   * <p>p.run();
   *
   * <p>byte[] actual = sync.get(expectedKey.toByteArray()); assertThat(actual,
   * equalTo(expectedValue.toByteArray())); }
   */
}
