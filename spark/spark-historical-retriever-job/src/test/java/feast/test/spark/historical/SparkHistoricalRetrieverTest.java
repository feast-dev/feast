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

import static org.hamcrest.Matchers.startsWith;

import com.google.protobuf.util.JsonFormat;
import feast.proto.core.FeatureSetProto.*;
import feast.proto.serving.ServingAPIProto.*;
import feast.spark.historical.SparkHistoricalRetrieverJob;
import feast.spark.ingestion.delta.FeatureRowToSparkRow;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkHistoricalRetrieverTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SparkHistoricalRetrieverTest.class.getName());
  private static final FeatureRowToSparkRow SparkFeatureConverter =
      new FeatureRowToSparkRow("myid");

  @Rule public TemporaryFolder dataFolder = new TemporaryFolder();

  @Rule public final SparkSessionRule spark = new SparkSessionRule();

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

    Dataset<Row> testDataset1 = spark.session.createDataFrame(testData1, schema1);
    Dataset<Row> testDataset2 = spark.session.createDataFrame(testData2, schema2);
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

    String entityDataPath = dataFolder.newFolder("entity_source").getAbsolutePath();
    entityDS.write().format("avro").mode("overwrite").save(entityDataPath);

    String testFolderPath1 =
        dataFolder.newFolder(testSpec1.getProject(), testSpec1.getName()).getAbsolutePath();
    LOGGER.info("Writing test data 1 to {}", testFolderPath1);
    featureDS1.write().format("delta").mode("overwrite").save(testFolderPath1);

    String testFolderPath2 =
        dataFolder.newFolder(testSpec2.getProject(), testSpec2.getName()).getAbsolutePath();
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

    String exportPath = dataFolder.newFolder("export_root").getAbsolutePath();
    String requestJson = JsonFormat.printer().omittingInsignificantWhitespace().print(request);
    SparkHistoricalRetrieverJob job =
        new SparkHistoricalRetrieverJob(
            new String[] {requestJson, dataFolder.getRoot().getAbsolutePath(), exportPath});

    String ent_table_name = job.loadSourceDataset(null);
    Long loadedTableCount = spark.session.table(ent_table_name).count();
    assert (spark.session.table(ent_table_name).count() == entityDS.count());
    LOGGER.info("Loaded table count {} == {}", loadedTableCount, entityDS.count());

    job.run();
    Dataset<Row> resultDS = spark.session.read().format("avro").load(exportPath);
    assert (entityDS.count() == resultDS.count());

    List<String> fieldNames =
        Arrays.stream(resultDS.schema().fieldNames()).collect(Collectors.toList());
    List<String> desiredFieldNames = new ArrayList<>();
    desiredFieldNames.add(String.format("%s__%s", testSpec1.getName(), savedFeature1.getName()));
    desiredFieldNames.add(String.format("%s__%s", testSpec2.getName(), savedFeature2.getName()));
    LOGGER.info("Job done, loading result table");
    assert (fieldNames.containsAll(desiredFieldNames));
  }

  public final class SparkSessionRule implements TestRule {
    public SparkSession session;

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          Assume.assumeThat(System.getProperty("java.version"), startsWith("1.8"));
          session =
              SparkSession.builder().appName(getClass().getName()).master("local").getOrCreate();
          try {
            base.evaluate(); // This will run the test.
          } finally {
            session.close();
          }
        }
      };
    }
  }
}
