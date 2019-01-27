/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.ingestion;

import static feast.FeastMatchers.hasCount;
import static feast.NormalizeFeatureRows.normalize;
import static feast.storage.MockFeatureErrorsStoreFactory.MOCK_ERRORS_STORE_TYPE;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.Timestamp;
import feast.ToOrderedFeatureRows;
import feast.ingestion.boot.ImportJobModule;
import feast.ingestion.boot.TestPipelineModule;
import feast.ingestion.model.Features;
import feast.ingestion.model.Values;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.service.SpecRetrievalException;
import feast.ingestion.util.ProtoUtil;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.storage.MockFeatureErrorsStoreFactory;
import feast.storage.MockServingStoreFactory;
import feast.storage.MockWarehouseStoreFactory;
import feast.storage.service.FeatureErrorsStoreFactoryService;
import feast.storage.service.FeatureServingStoreFactoryService;
import feast.storage.service.FeatureWarehouseStoreFactoryService;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Slf4j
public class ImportJobCSVTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  public ImportSpec initImportSpec(ImportSpec importSpec, String dataFile) {
    return importSpec.toBuilder().putSourceOptions("path", dataFile).build();
  }

  public ImportJobPipelineOptions initOptions() {
    Path path = Paths.get(Resources.getResource("core_specs/").getPath());
    ImportJobPipelineOptions options = PipelineOptionsFactory.create()
        .as(ImportJobPipelineOptions.class);
    options.setCoreApiSpecPath(path.toString());
    options.setErrorsStoreType(MOCK_ERRORS_STORE_TYPE);
    return options;
  }

  @Test
  public void testImportCSV() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file.csv\n"
                + "sourceOptions:\n"
                + "  # path: to be overwritten in tests\n"
                + "entities:\n"
                + "  - testEntity\n"
                + "schema:\n"
                + "  entityIdColumn: id\n"
                + "  timestampValue: 2018-09-25T00:00:00.000Z\n"
                + "  fields:\n"
                + "    - name: id\n"
                + "    - featureId: testEntity.none.testInt32\n"
                + "    - featureId: testEntity.none.testString\n"
                + "\n",
            ImportSpec.getDefaultInstance());

    File csvFile = folder.newFile("data.csv");
    Files.asCharSink(csvFile, Charsets.UTF_8).write("1,101,a\n2,202,b\n3,303,c\n");
    importSpec = initImportSpec(importSpec, csvFile.toString());

    ImportJobPipelineOptions options = initOptions();
    options.setErrorsStoreType(MOCK_ERRORS_STORE_TYPE);

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    ImportJob job = injector.getInstance(ImportJob.class);
    injector.getInstance(ImportJob.class);
    job.expand();

    PCollection<FeatureRowExtended> writtenToServing =
        PCollectionList.of(FeatureServingStoreFactoryService.get(MockServingStoreFactory.class).getWrite().getInputs())
            .apply("flatten serving input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToWarehouse =
        PCollectionList.of(
            FeatureWarehouseStoreFactoryService.get(MockWarehouseStoreFactory.class).getWrite().getInputs())
            .apply("flatten warehouse input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(FeatureErrorsStoreFactoryService.get(MockFeatureErrorsStoreFactory.class).getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    List<FeatureRow> expectedRows =
        Lists.newArrayList(
            normalize(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .setEntityKey("1")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testInt32", Values.ofInt32(101)))
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("a")))
                    .build()),
            normalize(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .setEntityKey("2")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testInt32", Values.ofInt32(202)))
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("b")))
                    .build()),
            normalize(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .setEntityKey("3")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testInt32", Values.ofInt32(303)))
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("c")))
                    .build()));

    PAssert.that(writtenToErrors).satisfies(hasCount(0));

    PAssert.that(writtenToServing.apply("serving toFeatureRows", new ToOrderedFeatureRows()))
        .containsInAnyOrder(expectedRows);

    PAssert.that(writtenToWarehouse.apply("warehouse toFeatureRows", new ToOrderedFeatureRows()))
        .containsInAnyOrder(expectedRows);

    testPipeline.run();
  }

  @Test
  public void testImportCSV_withSample1() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file.csv\n"
                + "sourceOptions:\n"
                + "  # path: to be overwritten in tests\n"
                + "jobOptions:\n"
                + "  sample.limit: 1\n"
                + "entities:\n"
                + "  - testEntity\n"
                + "schema:\n"
                + "  entityIdColumn: id\n"
                + "  timestampValue: 2018-09-25T00:00:00.000Z\n"
                + "  fields:\n"
                + "    - name: id\n"
                + "    - featureId: testEntity.none.testInt32\n"
                + "    - featureId: testEntity.none.testString\n"
                + "\n",
            ImportSpec.getDefaultInstance());

    File csvFile = folder.newFile("data.csv");
    Files.asCharSink(csvFile, Charsets.UTF_8).write("1,101,a\n2,202,b\n3,303,c\n");
    importSpec = initImportSpec(importSpec, csvFile.toString());

    ImportJobPipelineOptions options = initOptions();
    options.setErrorsStoreType(MOCK_ERRORS_STORE_TYPE);

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    ImportJob job = injector.getInstance(ImportJob.class);
    injector.getInstance(ImportJob.class);
    job.expand();

    PCollection<FeatureRowExtended> writtenToServing =
        PCollectionList.of(FeatureServingStoreFactoryService.get(MockServingStoreFactory.class).getWrite().getInputs())
            .apply("flatten serving input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToWarehouse =
        PCollectionList.of(
            FeatureWarehouseStoreFactoryService.get(MockWarehouseStoreFactory.class).getWrite().getInputs())
            .apply("flatten warehouse input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(FeatureErrorsStoreFactoryService.get(MockFeatureErrorsStoreFactory.class).getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    PAssert.that(writtenToServing).satisfies(hasCount(1));
    PAssert.that(writtenToWarehouse).satisfies(hasCount(1));
    PAssert.that(writtenToErrors).satisfies(hasCount(0));

    testPipeline.run();
  }

  @Test
  public void testImportCSV_withCoalesceRows() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file.csv\n"
                + "sourceOptions:\n"
                + "  # path: to be overwritten in tests\n"
                + "jobOptions:\n"
                + "  coalesceRows.enabled: true\n"
                + "entities:\n"
                + "  - testEntity\n"
                + "schema:\n"
                + "  entityIdColumn: id\n"
                + "  timestampColumn: timestamp\n"
                + "  fields:\n"
                + "    - name: id\n"
                + "    - name: timestamp\n"
                + "    - featureId: testEntity.none.testInt32\n"
                + "    - featureId: testEntity.none.testString\n"
                + "\n",
            ImportSpec.getDefaultInstance());

    File csvFile = folder.newFile("data.csv");
    Files.asCharSink(csvFile, Charsets.UTF_8)
        .write("1,2018-09-25T00:00:00.000Z,101,a\n1,2018-09-26T00:00:00.000Z,,b\n");
    importSpec = initImportSpec(importSpec, csvFile.toString());

    ImportJobPipelineOptions options = initOptions();
    options.setErrorsStoreType(MOCK_ERRORS_STORE_TYPE);

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    ImportJob job = injector.getInstance(ImportJob.class);
    injector.getInstance(ImportJob.class);
    job.expand();

    PCollection<FeatureRowExtended> writtenToServing =
        PCollectionList.of(FeatureServingStoreFactoryService.get(MockServingStoreFactory.class).getWrite().getInputs())
            .apply("flatten serving input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToWarehouse =
        PCollectionList.of(
            FeatureWarehouseStoreFactoryService.get(MockWarehouseStoreFactory.class).getWrite().getInputs())
            .apply("flatten warehouse input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(FeatureErrorsStoreFactoryService.get(MockFeatureErrorsStoreFactory.class).getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    PAssert.that(writtenToErrors).satisfies(hasCount(0));

    PAssert.that(writtenToServing.apply("serving toFeatureRows", new ToOrderedFeatureRows()))
        .containsInAnyOrder(
            normalize(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEntityKey("1")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testInt32", Values.ofInt32(101)))
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("b")))
                    .build()));

    PAssert.that(writtenToWarehouse.apply("warehouse toFeatureRows", new ToOrderedFeatureRows()))
        .containsInAnyOrder(
            normalize(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEntityKey("1")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testInt32", Values.ofInt32(101)))
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("a")))
                    .build()),
            normalize(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEntityKey("1")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("b")))
                    .build()));

    testPipeline.run();
  }

  @Test(expected = SpecRetrievalException.class)
  public void testImportCSVUnknownServingStoreError() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file.csv\n"
                + "sourceOptions:\n"
                + "  # path: to be overwritten in tests\n"
                + "entities:\n"
                + "  - testEntity\n"
                + "schema:\n"
                + "  entityIdColumn: id\n"
                + "  timestampValue: 2018-09-25T00:00:00.000Z\n"
                + "  fields:\n"
                + "    - name: id\n"
                + "    - featureId: testEntity.none.unknownInt32\n"
                // Unknown store is not available
                + "    - featureId: testEntity.none.testString\n"
                + "\n",
            ImportSpec.getDefaultInstance());

    File csvFile = folder.newFile("data.csv");
    Files.asCharSink(csvFile, Charsets.UTF_8).write("1,101,a\n2,202,b\n3,303,c\n");
    importSpec = initImportSpec(importSpec, csvFile.toString());

    ImportJobPipelineOptions options = initOptions();

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    ImportJob job = injector.getInstance(ImportJob.class);
    injector.getInstance(ImportJob.class);

    // Job should fail during expand(), so we don't even need to start the pipeline.
    job.expand();
  }

  @Test
  public void testImportWithErrors() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file.csv\n"
                + "sourceOptions:\n"
                + "  # path: to be overwritten in tests\n"
                + "entities:\n"
                + "  - testEntity\n"
                + "schema:\n"
                + "  entityIdColumn: id\n"
                + "  timestampValue: 2018-09-25T00:00:00.000Z\n"
                + "  fields:\n"
                + "    - name: id\n"
                + "    - featureId: testEntity.none.testString\n"
                + "    - featureId: testEntity.none.testInt32\n"
                + "\n",
            ImportSpec.getDefaultInstance());

    File csvFile = folder.newFile("data.csv");

    // Note the string and integer features are in the wrong positions for the import spec.
    Files.asCharSink(csvFile, Charsets.UTF_8).write("1,101,a\n2,202,b\n3,303,c\n");
    importSpec = initImportSpec(importSpec, csvFile.toString());

    ImportJobPipelineOptions options = initOptions();
    options.setErrorsStoreType(MOCK_ERRORS_STORE_TYPE);

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    ImportJob job = injector.getInstance(ImportJob.class);

    injector.getInstance(ImportJob.class);
    job.expand();

    PCollection<FeatureRowExtended> writtenToServing =
        PCollectionList.of(FeatureServingStoreFactoryService.get(MockServingStoreFactory.class).getWrite().getInputs())
            .apply("flatten serving input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(FeatureErrorsStoreFactoryService.get(MockFeatureErrorsStoreFactory.class).getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    PAssert.that(writtenToErrors)
        .satisfies(
            (errors) -> {
              int i = 0;
              for (FeatureRowExtended row : errors) {
                assertEquals(
                    row.getLastAttempt().getError().getCause(),
                    "feast.ingestion.exceptions.TypeConversionException");
                i += 1;
              }
              assertEquals(i, 3);
              return null;
            });

    PAssert.that(writtenToServing).satisfies(hasCount(0));
    testPipeline.run();
  }


  @Test
  public void testImportWithoutWarehouseStore() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file.csv\n"
                + "sourceOptions:\n"
                + "  # path: to be overwritten in tests\n"
                + "entities:\n"
                + "  - testEntity\n"
                + "schema:\n"
                + "  entityIdColumn: id\n"
                + "  timestampValue: 2018-09-25T00:00:00.000Z\n"
                + "  fields:\n"
                + "    - name: id\n"
                + "    - featureId: testEntity.none.testInt64NoWarehouse\n"
                + "    - featureId: testEntity.none.testStringNoWarehouse\n"
                + "\n",
            ImportSpec.getDefaultInstance());

    File csvFile = folder.newFile("data.csv");

    // Note the string and integer features are in the wrong positions for the import spec.
    Files.asCharSink(csvFile, Charsets.UTF_8).write("1,101,a\n2,202,b\n3,303,c\n");
    importSpec = initImportSpec(importSpec, csvFile.toString());

    ImportJobPipelineOptions options = initOptions();
    options.setErrorsStoreType(MOCK_ERRORS_STORE_TYPE);

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    ImportJob job = injector.getInstance(ImportJob.class);

    injector.getInstance(ImportJob.class);
    job.expand();

    PCollection<FeatureRowExtended> writtenToServing =
        PCollectionList.of(FeatureServingStoreFactoryService.get(MockServingStoreFactory.class).getWrite().getInputs())
            .apply("flatten serving input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(FeatureErrorsStoreFactoryService.get(MockFeatureErrorsStoreFactory.class).getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    PAssert.that(writtenToErrors)
        .satisfies(hasCount(0));

    PAssert.that(writtenToServing).satisfies(hasCount(3));
    testPipeline.run();
  }
}
