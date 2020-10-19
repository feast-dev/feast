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
package feast.test.spark.historical;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import com.google.protobuf.util.JsonFormat;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.serving.ServingAPIProto.DataFormat;
import feast.proto.serving.ServingAPIProto.DatasetSource;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.spark.HistoricalRetrievalProto.FeatureSetRequest;
import feast.proto.serving.spark.HistoricalRetrievalProto.HistoricalRetrievalRequest;
import feast.spark.historical.SparkHistoricalRetrieverJob;
import feast.spark.ingestion.delta.FeatureRowToSparkRow;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkHistoricalRetrieverTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SparkHistoricalRetrieverTest.class.getName());
  private static final FeatureRowToSparkRow SparkFeatureConverter =
      new FeatureRowToSparkRow("myid");

  @TempDir public Path rootDir;

  public SparkSession sparkSession;

  @BeforeEach
  public void setUpSpark() {
    sparkSession =
        SparkSession.builder()
            .appName(getClass().getName())
            .master("local")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
  }

  @AfterEach
  public void tearDownSpark() {
    if (sparkSession != null) {
      this.sparkSession.close();
    }
  }

  @Test
  public void testHistoricalJob() throws Exception {
    FeatureSet testFeatureSet1 =
        HistoricalTestUtil.createFeatureSetForDelta("fs_historical_test_1", "f_");
    FeatureSetSpec testSpec1 = testFeatureSet1.getSpec();

    FeatureSet testFeatureSet2 =
        HistoricalTestUtil.createFeatureSetForDelta("fs_historical_test_2", "g_");
    FeatureSetSpec testSpec2 = testFeatureSet2.getSpec();

    StructType schema1 = SparkFeatureConverter.buildSchema(testSpec1);
    StructType schema2 = SparkFeatureConverter.buildSchema(testSpec2);

    List<Row> testData1 =
        HistoricalTestUtil.generateTestData(testSpec1, 16).stream()
            .map(tRow -> SparkFeatureConverter.apply(testSpec1, tRow))
            .collect(Collectors.toList());
    List<Row> testData2 =
        HistoricalTestUtil.generateTestData(testSpec2, 16).stream()
            .map(tRow -> SparkFeatureConverter.apply(testSpec2, tRow))
            .collect(Collectors.toList());

    Dataset<Row> testDataset1 = sparkSession.createDataFrame(testData1, schema1);
    Dataset<Row> testDataset2 = sparkSession.createDataFrame(testData2, schema2);
    FeatureSpec savedFeature1 = testSpec1.getFeatures(1);
    FeatureSpec savedFeature2 = testSpec2.getFeatures(2);

    Dataset<Row> entityDS =
        testDataset1
            .select("event_timestamp", "entity_id_primary")
            .union(testDataset2.select("event_timestamp", "entity_id_primary"));
    Dataset<Row> featureDS1 =
        testDataset1.select(
            "event_timestamp", "entity_id_primary", "created_timestamp", savedFeature1.getName());
    Dataset<Row> featureDS2 =
        testDataset2.select(
            "event_timestamp", "entity_id_primary", "created_timestamp", savedFeature2.getName());

    String entityDataPath =
        Files.createDirectory(rootDir.resolve("entity_source")).toAbsolutePath().toString();
    entityDS.write().format("avro").mode("overwrite").save(entityDataPath);

    String testFolderPath1 =
        Files.createDirectories(
                rootDir.resolve(testSpec1.getProject()).resolve(testSpec1.getName()))
            .toAbsolutePath()
            .toString();
    LOGGER.info("Writing test data 1 to {}", testFolderPath1);
    featureDS1.write().format("delta").mode("overwrite").save(testFolderPath1);

    String testFolderPath2 =
        Files.createDirectories(
                rootDir.resolve(testSpec2.getProject()).resolve(testSpec2.getName()))
            .toAbsolutePath()
            .toString();
    LOGGER.info("Writing test data 2 to {}", testFolderPath2);
    featureDS2.write().format("delta").mode("overwrite").save(testFolderPath2);

    HistoricalRetrievalRequest request =
        HistoricalRetrievalRequest.newBuilder()
            .setRetrievalId("test_id")
            .setDatasetSource(
                DatasetSource.newBuilder()
                    .setFileSource(
                        DatasetSource.FileSource.newBuilder()
                            .setDataFormat(DataFormat.DATA_FORMAT_AVRO)
                            .addFileUris(entityDataPath)))
            .addFeatureSetRequests(
                FeatureSetRequest.newBuilder()
                    .setSpec(testSpec1)
                    .addFeatureReferences(
                        FeatureReference.newBuilder()
                            .setProject(testSpec1.getProject())
                            .setName(savedFeature1.getName())
                            .setFeatureSet(testSpec1.getName())
                            .build()))
            .addFeatureSetRequests(
                FeatureSetRequest.newBuilder()
                    .setSpec(testSpec2)
                    .addFeatureReferences(
                        FeatureReference.newBuilder()
                            .setProject(testSpec2.getProject())
                            .setName(savedFeature2.getName())
                            .setFeatureSet(testSpec2.getName())
                            .build())
                    .build())
            .build();

    String exportPath =
        Files.createDirectory(rootDir.resolve("export_root")).toAbsolutePath().toString();
    String requestJson = JsonFormat.printer().omittingInsignificantWhitespace().print(request);
    SparkHistoricalRetrieverJob job =
        new SparkHistoricalRetrieverJob(
            new String[] {requestJson, rootDir.toAbsolutePath().toString(), exportPath});

    String ent_table_name = job.loadSourceDataset(null);
    Long loadedTableCount = sparkSession.table(ent_table_name).count();
    assertThat(sparkSession.table(ent_table_name).count(), is(entityDS.count()));
    LOGGER.info("Loaded table count {} == {}", loadedTableCount, entityDS.count());

    job.run();
    Dataset<Row> resultDS = sparkSession.read().format("avro").load(exportPath);
    assertThat(entityDS.count(), is(resultDS.count()));

    List<String> fieldNames =
        Arrays.stream(resultDS.schema().fieldNames()).collect(Collectors.toList());
    List<String> desiredFieldNames = new ArrayList<>();
    desiredFieldNames.add("event_timestamp");
    desiredFieldNames.add("entity_id_primary");
    desiredFieldNames.add("uuid");
    desiredFieldNames.add(String.format("%s__%s", testSpec1.getName(), savedFeature1.getName()));
    desiredFieldNames.add(String.format("%s__%s", testSpec2.getName(), savedFeature2.getName()));
    LOGGER.info("Job done, loading result table");
    assertThat(fieldNames, containsInAnyOrder(desiredFieldNames.toArray()));
  }
}
