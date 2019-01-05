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
import feast.ingestion.options.ImportJobOptions;
import feast.ingestion.util.ProtoUtil;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.storage.MockErrorsStore;
import feast.storage.MockServingStore;
import feast.storage.MockWarehouseStore;
import feast.storage.service.ErrorsStoreService;
import feast.storage.service.ServingStoreService;
import feast.storage.service.WarehouseStoreService;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static feast.FeastMatchers.hasCount;
import static feast.ToOrderedFeatureRows.orderedFeatureRow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class ImportJobCSVTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  public ImportSpec initImportSpec(ImportSpec importSpec, String dataFile) {
    return importSpec.toBuilder().putOptions("path", dataFile).build();
  }

  public ImportJobOptions initOptions() {
    Path path = Paths.get(Resources.getResource("core_specs/").getPath());
    ImportJobOptions options = PipelineOptionsFactory.create().as(ImportJobOptions.class);
    options.setCoreApiSpecPath(path.toString());
    options.setErrorsStoreType(MockErrorsStore.MOCK_ERRORS_STORE_TYPE);
    return options;
  }

  @Test
  public void testImportCSV() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file\n"
                + "options:\n"
                + "  format: csv\n"
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

    ImportJobOptions options = initOptions();

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    MockErrorsStore mockErrorStore = new MockErrorsStore();
    ErrorsStoreService.register(mockErrorStore);

    ImportJob job = injector.getInstance(ImportJob.class);
    injector.getInstance(ImportJob.class);
    job.expand();

    PCollection<FeatureRowExtended> writtenToServing =
        PCollectionList.of(ServingStoreService.get(MockServingStore.class).getWrite().getInputs())
            .apply("flatten serving input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToWarehouse =
        PCollectionList.of(
                WarehouseStoreService.get(MockWarehouseStore.class).getWrite().getInputs())
            .apply("flatten warehouse input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(((MockErrorsStore) ErrorsStoreService.get()).getWrite().getInputs())
            .apply("flatten errors input", Flatten.pCollections());

    List<FeatureRow> expectedRows =
        Lists.newArrayList(
            orderedFeatureRow(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .setEntityKey("1")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testInt32", Values.ofInt32(101)))
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("a")))
                    .build()),
            orderedFeatureRow(
                FeatureRow.newBuilder()
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .setEntityKey("2")
                    .setEntityName("testEntity")
                    .addFeatures(Features.of("testEntity.none.testInt32", Values.ofInt32(202)))
                    .addFeatures(Features.of("testEntity.none.testString", Values.ofString("b")))
                    .build()),
            orderedFeatureRow(
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
  public void testImportCSVUnknownServingStoreError() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file\n"
                + "options:\n"
                + "  format: csv\n"
                + "  # path: to be overwritten in tests\n"
                + "entities:\n"
                + "  - testEntity\n"
                + "schema:\n"
                + "  entityIdColumn: id\n"
                + "  timestampValue: 2018-09-25T00:00:00.000Z\n"
                + "  fields:\n"
                + "    - name: id\n"
                + "    - featureId: testEntity.none.redisInt32\n" // Redis is not available by
                // default from the json specs
                + "    - featureId: testEntity.none.testString\n"
                + "\n",
            ImportSpec.getDefaultInstance());

    File csvFile = folder.newFile("data.csv");
    Files.asCharSink(csvFile, Charsets.UTF_8).write("1,101,a\n2,202,b\n3,303,c\n");
    importSpec = initImportSpec(importSpec, csvFile.toString());

    ImportJobOptions options = initOptions();

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    ImportJob job = injector.getInstance(ImportJob.class);
    injector.getInstance(ImportJob.class);

    // Job should fail during expand(), so we don't even need to start the pipeline.
    try {
      job.expand();
      fail("Should not reach here, we should have thrown an exception");
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Feature testEntity.none.redisInt32 references unknown serving store REDIS1",
          e.getMessage());
    }
  }

  @Test
  public void testImportWithErrors() throws IOException {
    ImportSpec importSpec =
        ProtoUtil.decodeProtoYaml(
            "---\n"
                + "type: file\n"
                + "options:\n"
                + "  format: csv\n"
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

    ImportJobOptions options = initOptions();

    Injector injector =
        Guice.createInjector(
            new ImportJobModule(options, importSpec), new TestPipelineModule(testPipeline));

    MockErrorsStore mockErrorStore = new MockErrorsStore();
    ErrorsStoreService.register(mockErrorStore);

    ImportJob job = injector.getInstance(ImportJob.class);

    injector.getInstance(ImportJob.class);
    job.expand();

    PCollection<FeatureRowExtended> writtenToServing =
        PCollectionList.of(ServingStoreService.get(MockServingStore.class).getWrite().getInputs())
            .apply("flatten serving input", Flatten.pCollections());

    PCollection<FeatureRowExtended> writtenToErrors =
        PCollectionList.of(((MockErrorsStore) ErrorsStoreService.get()).getWrite().getInputs())
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
}
