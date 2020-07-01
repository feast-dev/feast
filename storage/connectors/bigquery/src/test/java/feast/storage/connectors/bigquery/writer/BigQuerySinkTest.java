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
package feast.storage.connectors.bigquery.writer;

import static feast.storage.common.testing.TestUtil.createRandomValue;
import static feast.storage.common.testing.TestUtil.field;
import static feast.storage.connectors.bigquery.writer.FeatureSetSpecToTableSchema.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import feast.storage.connectors.bigquery.compression.CompactFeatureRows;
import feast.storage.connectors.bigquery.compression.FeatureRowsBatch;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BatchLoadsWithResult;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class BigQuerySinkTest {

  @Rule public transient TestPipeline p = TestPipeline.fromOptions(makePipelineOptions());
  @Rule public final ExpectedException exception = ExpectedException.none();

  @Mock(serializable = true)
  private BigQuery bigQuery;

  private FakeJobService jobService = new FakeJobService();
  private FakeDatasetService datasetService = new FakeDatasetService();
  private Random rd = new Random();

  List<TableFieldSchema> commonFields =
      Arrays.asList(
          new TableFieldSchema()
              .setName("event_timestamp")
              .setType("TIMESTAMP")
              .setDescription(BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION),
          new TableFieldSchema()
              .setName("created_timestamp")
              .setType("TIMESTAMP")
              .setDescription(BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION),
          new TableFieldSchema()
              .setName("ingestion_id")
              .setType("STRING")
              .setDescription(BIGQUERY_INGESTION_ID_FIELD_DESCRIPTION),
          new TableFieldSchema()
              .setName("job_id")
              .setType("STRING")
              .setDescription(BIGQUERY_JOB_ID_FIELD_DESCRIPTION));
  FeatureSetSpec spec;

  public static PipelineOptions makePipelineOptions() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setTempLocation("/tmp/feast");
    return options;
  }

  private FeatureRow generateRow(String featureSet) {
    FeatureRow.Builder row =
        FeatureRow.newBuilder()
            .setFeatureSet(featureSet)
            .addFields(field("entity", rd.nextInt(), ValueProto.ValueType.Enum.INT64))
            .addFields(FieldProto.Field.newBuilder().setName("null_value").build());

    for (ValueProto.ValueType.Enum type : ValueProto.ValueType.Enum.values()) {
      if (type == ValueProto.ValueType.Enum.INVALID
          || type == ValueProto.ValueType.Enum.UNRECOGNIZED) {
        continue;
      }
      row.addFields(
          FieldProto.Field.newBuilder()
              .setName(String.format("feature_%d", type.getNumber()))
              .setValue(createRandomValue(type, 5))
              .build());
    }

    return row.build();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    initMocks(this);

    Logger.getLogger(BatchLoadsWithResult.class.getName()).addHandler(new ConsoleHandler());

    when(bigQuery.getTable(TableId.of("test-project", "test_dataset", "myproject_fs")))
        .thenReturn(null);

    FakeDatasetService.setUp();
    datasetService.createDataset("test-project", "test_dataset", "us-central1", "description", 0L);

    spec =
        FeatureSetSpec.newBuilder()
            .setName("fs")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity")
                    .setValueType(ValueProto.ValueType.Enum.INT64)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder()
                    .setName("feature")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .build();
  }

  private FeatureSink makeSink(
      ValueProvider<BigQuery> bq, PCollection<KV<FeatureSetReference, FeatureSetSpec>> specs) {
    BigQueryFeatureSink sink =
        BigQueryFeatureSink.builder()
            .setDatasetId("test_dataset")
            .setProjectId("test-project")
            .setBQTestServices(
                new FakeBigQueryServices()
                    .withJobService(jobService)
                    .withDatasetService(datasetService))
            .setBQClient(bq)
            .setTriggeringFrequency(Duration.standardSeconds(5))
            .build();
    sink.prepareWrite(specs);
    return sink;
  }

  @Test
  public void simpleInsert() {
    FeatureRow row1 = generateRow("myproject/fs");
    FeatureRow row2 = generateRow("myproject/fs");

    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .advanceWatermarkTo(Instant.now())
            .addElements(row1, row2)
            .advanceWatermarkToInfinity();

    FeatureSink sink =
        makeSink(
            ValueProvider.StaticValueProvider.of(bigQuery),
            p.apply(
                Create.of(
                    ImmutableMap.of(
                        FeatureSetReference.of(spec.getProject(), spec.getName(), 1), spec))));
    PCollection<FeatureRow> successfulInserts =
        p.apply(featureRowTestStream).apply(sink.writer()).getSuccessfulInserts();

    List<FeatureRow> inputWithoutNulls = dropNullFeature(ImmutableList.of(row1, row2));
    PAssert.that(successfulInserts).containsInAnyOrder(inputWithoutNulls);
    p.run();

    assert jobService.getAllJobs().size() == 1;
    Job load = Iterators.getLast(jobService.getAllJobs().iterator());
    JobConfigurationLoad loadConfiguration = load.getConfiguration().getLoad();

    ArrayList<TableFieldSchema> expectedFields =
        new ArrayList<>(
            Arrays.asList(
                new TableFieldSchema().setName("entity").setType("INTEGER"),
                new TableFieldSchema().setName("feature").setType("STRING")));

    expectedFields.addAll(commonFields);

    assertThat(loadConfiguration.getSchema().getFields(), is(expectedFields));

    assertThat(
        loadConfiguration.getDestinationTable(),
        is(
            new TableReference()
                .setDatasetId("test_dataset")
                .setProjectId("test-project")
                .setTableId("myproject_fs")));
  }

  @Test
  public void uniqueJobIdPerWindow() {
    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .advanceWatermarkTo(Instant.now())
            .addElements(generateRow("myproject/fs"))
            .addElements(generateRow("myproject/fs"))
            .advanceWatermarkTo(Instant.now().plus(Duration.standardSeconds(10)))
            .addElements(generateRow("myproject/fs"))
            .addElements(generateRow("myproject/fs"))
            .advanceWatermarkToInfinity();

    FeatureSink sink =
        makeSink(
            ValueProvider.StaticValueProvider.of(bigQuery),
            p.apply(
                "StaticSpecs",
                Create.of(
                    ImmutableMap.of(
                        FeatureSetReference.of(spec.getProject(), spec.getName(), 1), spec))));

    p.apply(featureRowTestStream).apply(sink.writer());
    p.run();

    assertThat(jobService.getAllJobs().size(), is(2));
    assertThat(
        jobService.getAllJobs().stream()
            .map(j -> j.getJobReference().getJobId())
            .distinct()
            .count(),
        is(2L));
  }

  @Test
  public void expectingJobResult() {
    FeatureRow featureRow = generateRow("myproject/fs");
    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .advanceWatermarkTo(Instant.now())
            .addElements(featureRow)
            .advanceWatermarkToInfinity();

    jobService.setNumFailuresExpected(3);

    FeatureSink sink =
        makeSink(
            ValueProvider.StaticValueProvider.of(bigQuery),
            p.apply(
                "StaticSpecs",
                Create.of(
                    ImmutableMap.of(
                        FeatureSetReference.of(spec.getProject(), spec.getName(), 1), spec))));

    PTransform<PCollection<FeatureRow>, WriteResult> writer =
        ((BigQueryWrite) sink.writer()).withExpectingResultTime(Duration.standardSeconds(5));
    PCollection<FeatureRow> inserts =
        p.apply(featureRowTestStream).apply(writer).getSuccessfulInserts();

    PAssert.that(inserts).containsInAnyOrder(dropNullFeature(ImmutableList.of(featureRow)));

    p.run();
  }

  @Test
  public void updateSchemaWithExistingTable() {
    TableId tableId = TableId.of("test-project", "test_dataset", "myproject_fs_2");

    when(bigQuery.getTable(tableId))
        .thenAnswer(
            new TableAnswer(
                TableId.of("test-project", "test_dataset", "myproject_fs_2"),
                StandardTableDefinition.of(
                    Schema.of(
                        Field.of("old_feature_1", LegacySQLTypeName.FLOAT),
                        Field.of("old_feature_2", LegacySQLTypeName.INTEGER)))));

    FeatureSetSpec spec_fs_2 =
        FeatureSetSpec.newBuilder()
            .setName("fs_2")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity")
                    .setValueType(ValueProto.ValueType.Enum.INT64)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder()
                    .setName("feature")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .build();

    FeatureSink sink =
        makeSink(
            ValueProvider.StaticValueProvider.of(bigQuery),
            p.apply(
                Create.of(
                    ImmutableMap.of(
                        FeatureSetReference.of(spec_fs_2.getProject(), spec_fs_2.getName(), 1),
                        spec_fs_2))));

    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .advanceWatermarkTo(Instant.now())
            .addElements(generateRow("myproject/fs_2"))
            .advanceWatermarkToInfinity();

    p.apply(featureRowTestStream).apply(sink.writer());
    p.run();

    assert jobService.getAllJobs().size() == 1;
    Job load = Iterators.getLast(jobService.getAllJobs().iterator());
    JobConfigurationLoad loadConfiguration = load.getConfiguration().getLoad();

    ArrayList<TableFieldSchema> expectedFields =
        new ArrayList<>(
            Arrays.asList(
                new TableFieldSchema().setName("old_feature_1").setType("FLOAT"),
                new TableFieldSchema().setName("old_feature_2").setType("INTEGER"),
                new TableFieldSchema().setName("entity").setType("INTEGER"),
                new TableFieldSchema().setName("feature").setType("STRING")));
    expectedFields.addAll(commonFields);
    assertThat(loadConfiguration.getSchema().getFields(), is(expectedFields));
  }

  @Test
  public void updateSpecInFlight() {
    FeatureSetSpec spec_fs_2 =
        FeatureSetSpec.newBuilder()
            .setName("fs_2")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity")
                    .setValueType(ValueProto.ValueType.Enum.INT64)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder()
                    .setName("feature")
                    .setValueType(ValueProto.ValueType.Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder()
                    .setName("new_feature")
                    .setValueType(ValueProto.ValueType.Enum.FLOAT)
                    .build())
            .build();

    TestStream<KV<FeatureSetReference, FeatureSetSpec>> specsStream =
        TestStream.create(
                KvCoder.of(
                    AvroCoder.of(FeatureSetReference.class), ProtoCoder.of(FeatureSetSpec.class)))
            .advanceWatermarkTo(Instant.now())
            .addElements(KV.of(FeatureSetReference.of("myproject", "fs", 1), spec))
            .advanceProcessingTime(Duration.standardSeconds(5))
            // .advanceWatermarkTo(Instant.now().plus(Duration.standardSeconds(5)))
            .addElements(KV.of(FeatureSetReference.of("myproject", "fs", 1), spec_fs_2))
            .advanceWatermarkToInfinity();

    FeatureSink sink =
        makeSink(
            ValueProvider.StaticValueProvider.of(bigQuery),
            p.apply("SpecsInput", specsStream)
                .apply(
                    Window.<KV<FeatureSetReference, FeatureSetSpec>>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.millis(0))
                        .accumulatingFiredPanes()));

    TestStream<FeatureRow> featureRowTestStream =
        TestStream.create(ProtoCoder.of(FeatureRow.class))
            .advanceWatermarkTo(Instant.now().plus(Duration.standardSeconds(10)))
            .advanceProcessingTime(Duration.standardSeconds(10))
            .addElements(generateRow("myproject/fs"))
            .advanceWatermarkToInfinity();

    p.apply("FeaturesInput", featureRowTestStream).apply(sink.writer());
    p.run();

    assert jobService.getAllJobs().size() == 1;
    Job load = Iterators.getLast(jobService.getAllJobs().iterator());
    JobConfigurationLoad loadConfiguration = load.getConfiguration().getLoad();

    ArrayList<TableFieldSchema> expectedFields =
        new ArrayList<>(
            Arrays.asList(
                new TableFieldSchema().setName("entity").setType("INTEGER"),
                new TableFieldSchema().setName("feature").setType("STRING"),
                new TableFieldSchema().setName("new_feature").setType("FLOAT")));
    expectedFields.addAll(commonFields);

    assertThat(loadConfiguration.getSchema().getFields(), is(expectedFields));
  }

  public static class ExtractKV extends DoFn<FeatureRow, KV<String, FeatureRow>> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(KV.of(c.element().getFeatureSet(), c.element()));
    }
  }

  public static class FlatMap extends DoFn<KV<String, FeatureRowsBatch>, FeatureRow> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.element().getValue().getFeatureRows().forEachRemaining(c::output);
    }
  }

  @Test
  public void featureRowCompressShouldPackAndUnpackSuccessfully() {
    Stream<FeatureRow> stream1 = IntStream.range(0, 1000).mapToObj(i -> generateRow("project/fs"));
    Stream<FeatureRow> stream2 =
        IntStream.range(0, 1000).mapToObj(i -> generateRow("project/fs_2"));

    List<FeatureRow> input = Stream.concat(stream1, stream2).collect(Collectors.toList());

    FeatureRow rowWithNull =
        FeatureRow.newBuilder()
            .setFeatureSet("project/fs")
            .addAllFields(copyFieldsWithout(generateRow(""), "entity"))
            .addFields(FieldProto.Field.newBuilder().setName("entity").build())
            .build();

    List<FeatureRow> inputWithNulls = Lists.newArrayList(input);
    inputWithNulls.add(rowWithNull);

    PCollection<FeatureRow> result =
        p.apply(Create.of(inputWithNulls))
            .apply("KV", ParDo.of(new ExtractKV()))
            .apply(new CompactFeatureRows(1000))
            .apply("Flat", ParDo.of(new FlatMap()));

    List<FeatureRow> inputWithoutNulls = dropNullFeature(input);

    inputWithoutNulls.add(
        FeatureRow.newBuilder()
            .setFeatureSet("project/fs")
            .addFields(
                FieldProto.Field.newBuilder()
                    .setName("entity")
                    .setValue(ValueProto.Value.newBuilder().setInt64Val(0L).build())
                    .build())
            .addAllFields(copyFieldsWithout(rowWithNull, "entity", "null_value"))
            .build());

    PAssert.that(result).containsInAnyOrder(inputWithoutNulls);
    p.run();
  }

  private List<FeatureRow> dropNullFeature(List<FeatureRow> input) {
    return input.stream()
        .map(
            r ->
                FeatureRow.newBuilder()
                    .setFeatureSet(r.getFeatureSet())
                    .addAllFields(
                        r.getFieldsList().stream()
                            .filter(f -> !f.getName().equals("null_value"))
                            .collect(Collectors.toList()))
                    .build())
        .collect(Collectors.toList());
  }

  private List<FieldProto.Field> copyFieldsWithout(FeatureRow row, String... except) {
    ArrayList<String> exclude = Lists.newArrayList(except);
    return row.getFieldsList().stream()
        .filter(f -> !exclude.contains(f.getName()))
        .collect(Collectors.toList());
  }

  public static class TableAnswer implements Answer<Table>, Serializable {
    TableId tableId;
    TableDefinition tableDefinition;

    public TableAnswer(TableId tableId, TableDefinition tableDefinition) {
      this.tableId = tableId;
      this.tableDefinition = tableDefinition;
    }

    @Override
    public Table answer(InvocationOnMock invocationOnMock) throws Throwable {
      return FakeTable.create(mock(BigQuery.class), tableId, tableDefinition);
    }
  }
}
