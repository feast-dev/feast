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

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto;
import feast.proto.storage.BigtableProto.BigtableKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType.Enum;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigtableFeatureSinkTest {
  @Rule public transient TestPipeline p = TestPipeline.create();
  // Initialize the emulator Rule
  @Rule public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

  private static String BIGTABLE = "test_table";
  private static int REDIS_PORT = 51234;
  private BigtableTableAdminClient tableAdminClient;
  private BigtableDataClient dataClient;
  private BigtableFeatureSink bigtableFeatureSink;

  @Before
  public void setUp() throws IOException {
    // Initialize the clients to connect to the emulator
    BigtableTableAdminSettings.Builder tableAdminSettings =
        BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort());
    tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build());

    BigtableDataSettings.Builder dataSettings =
        BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort());
    dataClient = BigtableDataClient.create(dataSettings.build());

    // Create a test table that can be used in tests
    tableAdminClient.createTable(CreateTableRequest.of("fake-table").addFamily("cf"));
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
    StoreProto.Store.BigtableConfig bigtableConfig =
        StoreProto.Store.BigtableConfig.newBuilder().build();

    bigtableFeatureSink =
        BigtableFeatureSink.builder()
            .setFeatureSetSpecs(specMap)
            .setBigtableConfig(bigtableConfig)
            .build();
  }

  @Test
  public void testWriteRead() throws ExecutionException, InterruptedException {
    ApiFuture<Void> mutateFuture =
        dataClient.mutateRowAsync(
            RowMutation.create("fake-table", "fake-key").setCell("cf", "col", "value"));

    mutateFuture.get();

    ApiFuture<Row> rowFuture = dataClient.readRowAsync("fake-table", "fake-key");

    // Assert.assertEquals("value", rowFuture.get().getCells().get(0).getValue().toStringUtf8());
  }

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

    p.apply(Create.of(featureRows)).apply(bigtableFeatureSink.writer());
    p.run();

    kvs.forEach(
        (key, value) -> {
          Row actual = dataClient.readRow(BIGTABLE, key.toByteString());
          // assertThat(actual.getCells("feature", "latest"), equalTo(value.toByteString()));
        });
  }
}
/**
 * @Test(timeout = 10000) public void shouldRetryFailConnection() throws InterruptedException {
 * BigtableConfig bigtableConfig = BigtableConfig.newBuilder() .setHost(REDIS_HOST)
 * .setPort(REDIS_PORT) .setMaxRetries(4) .setInitialBackoffMs(2000) .build(); bigtableFeatureSink =
 * bigtableFeatureSink.toBuilder().setBigtableConfig(bigtableConfig).build();
 *
 * <p>HashMap<BigtableKey, FeatureRow> kvs = new LinkedHashMap<>(); kvs.put(
 * BigtableKey.newBuilder() .setFeatureSet("myproject/fs") .addEntities(field("entity", 1,
 * Enum.INT64)) .build(), FeatureRow.newBuilder() .setEventTimestamp(Timestamp.getDefaultInstance())
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
 * <p>kvs.forEach( (key, value) -> { byte[] actual = sync.get(key.toByteArray()); assertThat(actual,
 * equalTo(value.toByteArray())); }); } @Test public void
 * shouldProduceFailedElementIfRetryExceeded() {
 *
 * <p>BigtableConfig bigtableConfig =
 * BigtableConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT + 1).build();
 * bigtableFeatureSink = bigtableFeatureSink.toBuilder().setBigtableConfig(bigtableConfig).build();
 *
 * <p>HashMap<BigtableKey, FeatureRow> kvs = new LinkedHashMap<>(); kvs.put(
 * BigtableKey.newBuilder() .setFeatureSet("myproject/fs") .addEntities(field("entity", 1,
 * Enum.INT64)) .build(), FeatureRow.newBuilder() .setEventTimestamp(Timestamp.getDefaultInstance())
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
 * expectedValue = FeatureRow.newBuilder() .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
 * .addAllFields(expectedFields) .build();
 *
 * <p>p.apply(Create.of(offendingRow)).apply(bigtableFeatureSink.writer());
 *
 * <p>p.run();
 *
 * <p>byte[] actual = sync.get(expectedKey.toByteArray()); assertThat(actual,
 * equalTo(expectedValue.toByteArray())); } @Test public void shouldMergeDuplicateFeatureFields() {
 * FeatureRow featureRowWithDuplicatedFeatureFields = FeatureRow.newBuilder()
 * .setFeatureSet("myproject/feature_set") .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
 * .addFields( Field.newBuilder() .setName("entity_id_primary")
 * .setValue(Value.newBuilder().setInt32Val(1))) .addFields( Field.newBuilder()
 * .setName("entity_id_secondary") .setValue(Value.newBuilder().setStringVal("a"))) .addFields(
 * Field.newBuilder() .setName("feature_1") .setValue(Value.newBuilder().setStringVal("strValue1")))
 * .addFields( Field.newBuilder() .setName("feature_1")
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
 * .setFeatureSet("myproject/feature_set") .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
 * .addFields( Field.newBuilder() .setName("entity_id_primary")
 * .setValue(Value.newBuilder().setInt32Val(1))) .addFields( Field.newBuilder()
 * .setName("entity_id_secondary") .setValue(Value.newBuilder().setStringVal("a"))) .addFields(
 * Field.newBuilder() .setName("feature_1") .setValue(Value.newBuilder().setStringVal("strValue1")))
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
 * .addFields(Field.newBuilder().setValue(Value.getDefaultInstance())) .build();
 *
 * <p>p.apply(Create.of(featureRowWithDuplicatedFeatureFields)).apply(bigtableFeatureSink.writer());
 *
 * <p>p.run();
 *
 * <p>byte[] actual = sync.get(expectedKey.toByteArray()); assertThat(actual,
 * equalTo(expectedValue.toByteArray())); } }*
 */
