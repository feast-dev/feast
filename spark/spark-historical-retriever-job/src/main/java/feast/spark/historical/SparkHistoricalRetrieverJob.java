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
package feast.spark.historical;

import static org.apache.spark.sql.functions.monotonicallyIncreasingId;

import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.mitchellbosecke.pebble.PebbleEngine;
import com.mitchellbosecke.pebble.template.PebbleTemplate;
import feast.proto.serving.ServingAPIProto.DataFormat;
import feast.proto.serving.ServingAPIProto.DatasetSource;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.spark.HistoricalRetrievalProto.FeatureSetRequest;
import feast.proto.serving.spark.HistoricalRetrievalProto.HistoricalRetrievalRequest;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import scala.Tuple2;
import scala.collection.JavaConverters;

public class SparkHistoricalRetrieverJob {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(SparkHistoricalRetrieverJob.class);
  private final DatasetSource datasetSource;
  private final List<FeatureSetRequest> featureSetRequests;
  private final String deltaPath;
  private final String exportDestination;
  private final SparkSession sparkSession;

  private final PebbleEngine engine = new PebbleEngine.Builder().build();

  private static final String FEATURESET_TEMPLATE_NAME =
      "templates/single_featureset_pit_join.spark.sql";
  private static final String JOIN_TEMPLATE_NAME = "templates/join_featuresets.spark.sql";

  /**
   * Run a Spark historical retriever job.
   *
   * @param args List of parameters:
   *     <p>request-json A HistoricalRetrievalRequest in Protobuf JSON format.
   *     <p>delta-path Storage path for Delta Lake files.
   *     <p>export-destination Storage path for output.
   */
  public static void main(String[] args) throws Exception {
    SparkHistoricalRetrieverJob retriever = new SparkHistoricalRetrieverJob(args);
    retriever.run();
  }

  /**
   * Build a Spark historical retriever job.
   *
   * @param args List of Spark parameters.
   */
  public SparkHistoricalRetrieverJob(String[] args) throws InvalidProtocolBufferException {
    int numArgs = 3;
    if (args.length != numArgs) {
      throw new IllegalArgumentException("Expecting " + numArgs + " arguments");
    }

    int index = 0;
    String requestJson = args[index++];
    deltaPath = args[index++];
    exportDestination = args[index];

    HistoricalRetrievalRequest.Builder requestBuilder = HistoricalRetrievalRequest.newBuilder();
    JsonFormat.parser().merge(requestJson, requestBuilder);
    HistoricalRetrievalRequest request = requestBuilder.build();

    log.info("Delta path: {}", deltaPath);
    log.info("Export destination: {}", exportDestination);
    log.info("Request: {}", request);

    datasetSource = request.getDatasetSource();
    featureSetRequests = request.getFeatureSetRequestsList();

    sparkSession = SparkSession.builder().appName("SparkIngestion").getOrCreate();
  }

  public void run() throws Exception {
    List<FeatureSetQueryInfo> featureSetQueryInfos = getFeatureSetInfos(featureSetRequests);

    String entityTableName = loadSourceDataset(null);
    List<String> fieldNames = getDatasetFieldNames(entityTableName);
    log.info("Source files loaded as temp table `{}`", entityTableName);

    Tuple2<Timestamp, Timestamp> timestampRange = getDatasetTimestampRange(entityTableName);
    Timestamp min = timestampRange._1;
    Timestamp max = timestampRange._2;
    log.info("Timestamp range for input data set is {} -> {}", min, max);

    List<FeatureSetQueryInfo> updatedInfos;
    updatedInfos = generateFeatureSetDatasets(entityTableName, min, max, featureSetQueryInfos);

    Dataset<Row> finalJoin = generateFinalJoinDataset(updatedInfos, fieldNames, entityTableName);

    finalJoin.createOrReplaceTempView("final_join");
    writeDatasetToStore(finalJoin);
  }

  public String loadSourceDataset(String tempSourceTableName) {
    String entityTableName;
    if (tempSourceTableName == null) {
      entityTableName = createTempTableName();
    } else {
      entityTableName = tempSourceTableName;
    }

    Dataset<Row> sourceDS;

    DatasetSource.FileSource fileSource = this.datasetSource.getFileSource();
    String[] sourceFiles = fileSource.getFileUrisList().toArray(new String[0]);
    log.debug("Loading input files from {}", Arrays.toString(sourceFiles));

    if (fileSource.getDataFormat() != DataFormat.DATA_FORMAT_AVRO) {
      throw new RuntimeException("Unknown input format " + fileSource.getDataFormat());
    }
    sourceDS = sparkSession.read().format("avro").load(sourceFiles);
    sourceDS
        .withColumn("uuid", monotonicallyIncreasingId())
        .createOrReplaceTempView(entityTableName);

    log.info("Source files loaded as temp table `{}`", entityTableName);
    return entityTableName;
  }

  public List<String> getDatasetFieldNames(String sourceDatasetTableName) {
    Dataset<Row> sourceDataset = sparkSession.table(sourceDatasetTableName);
    return JavaConverters.asJavaCollection(sourceDataset.schema()).stream()
        .map(StructField::name)
        .filter(fieldName -> !"event_timestamp".equals(fieldName))
        .collect(Collectors.toList());
  }

  public Tuple2<Timestamp, Timestamp> getDatasetTimestampRange(String entityTableName) {
    Timestamp min, max;
    Row timestampLimitResult = sparkSession.sql(createTimestampLimitQuery(entityTableName)).first();
    max = timestampLimitResult.getTimestamp(0);
    min = timestampLimitResult.getTimestamp(1);

    return new Tuple2<>(min, max);
  }

  public List<FeatureSetQueryInfo> generateFeatureSetDatasets(
      String entityTableName,
      Timestamp minTime,
      Timestamp maxTime,
      List<FeatureSetQueryInfo> featureSetQueryInfos)
      throws IOException {
    List<FeatureSetQueryInfo> newInfos = new ArrayList<>();
    for (FeatureSetQueryInfo featureSetInfo : featureSetQueryInfos) {
      sparkSession
          .read()
          .format("delta")
          .load(
              String.format(
                  "%s/%s/%s", deltaPath, featureSetInfo.getProject(), featureSetInfo.getName()))
          .createOrReplaceTempView(
              String.format("tbl__%s_%s", featureSetInfo.getProject(), featureSetInfo.getName()));
      String query =
          createFeatureSetPointInTimeQuery(featureSetInfo, entityTableName, minTime, maxTime);
      String queryTableName = createTempTableName();
      log.info("Executing retrieval query into `{}`: {}", queryTableName, query);
      sparkSession.sql(query).createOrReplaceTempView(queryTableName);
      newInfos.add(new FeatureSetQueryInfo(featureSetInfo, queryTableName));
    }
    return newInfos;
  }

  public Dataset<Row> generateFinalJoinDataset(
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> fieldNames,
      String entityTableName)
      throws IOException {
    String joinQuery = createJoinQuery(featureSetQueryInfos, fieldNames, entityTableName);
    log.info("Executing join query: {}", joinQuery);
    return sparkSession.sql(joinQuery);
  }

  public void writeDatasetToStore(Dataset<Row> dataset) {
    log.info("Writing to store {}", exportDestination);
    // Use deflate compression for Python compatibility (snappy requires extra library)
    dataset
        .write()
        .format("avro")
        .option("compression", "deflate")
        .mode("append")
        .save(exportDestination);
  }

  public List<FeatureSetQueryInfo> getFeatureSetInfos(List<FeatureSetRequest> featureSetRequests)
      throws IllegalArgumentException {

    List<FeatureSetQueryInfo> featureSetInfos = new ArrayList<>();
    for (FeatureSetRequest featureSetRequest : featureSetRequests) {
      feast.proto.core.FeatureSetProto.FeatureSetSpec spec = featureSetRequest.getSpec();
      Duration maxAge = spec.getMaxAge();
      List<String> fsEntities =
          spec.getEntitiesList().stream()
              .map(feast.proto.core.FeatureSetProto.EntitySpec::getName)
              .collect(Collectors.toList());
      List<FeatureReference> features = featureSetRequest.getFeatureReferencesList();
      featureSetInfos.add(
          new FeatureSetQueryInfo(
              spec.getProject(), spec.getName(), maxAge.getSeconds(), fsEntities, features, ""));
    }
    return featureSetInfos;
  }

  /**
   * Generate the query for point in time correctness join of data for a single feature set to the
   * entity dataset.
   *
   * @param featureSetInfo Information about the feature set necessary for the query templating
   * @param leftTableName entity dataset name
   * @param minTimestamp earliest allowed timestamp for the historical data in feast
   * @param maxTimestamp latest allowed timestamp for the historical data in feast
   * @return point in time correctness join SQL query
   */
  private String createFeatureSetPointInTimeQuery(
      FeatureSetQueryInfo featureSetInfo,
      String leftTableName,
      Timestamp minTimestamp,
      Timestamp maxTimestamp)
      throws IOException {

    PebbleTemplate template = engine.getTemplate(FEATURESET_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("dataLocation", deltaPath);
    context.put("featureSet", featureSetInfo);
    context.put("minTimestamp", minTimestamp);
    context.put("maxTimestamp", maxTimestamp);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  /**
   * @param featureSetInfos List of FeatureSetInfos containing information about the feature set
   *     necessary for the query templating
   * @param entityTableColumnNames list of column names in entity table
   * @param leftTableName entity dataset name
   * @return query to join temporary feature set tables to the entity table
   */
  public String createJoinQuery(
      List<FeatureSetQueryInfo> featureSetInfos,
      List<String> entityTableColumnNames,
      String leftTableName)
      throws IOException {
    PebbleTemplate template = engine.getTemplate(JOIN_TEMPLATE_NAME);
    Map<String, Object> context = new HashMap<>();
    context.put("entities", entityTableColumnNames);
    context.put("featureSets", featureSetInfos);
    context.put("leftTableName", leftTableName);

    Writer writer = new StringWriter();
    template.evaluate(writer, context);
    return writer.toString();
  }

  /**
   * Get the query for retrieving the earliest and latest timestamps in the entity dataset.
   *
   * @param leftTableName full entity dataset name
   * @return timestamp limit BQ SQL query
   */
  public String createTimestampLimitQuery(String leftTableName) {
    return String.format(
        "SELECT CAST(MAX(event_timestamp) as TIMESTAMP) as max, CAST(MIN(event_timestamp) as TIMESTAMP) as min FROM `%s`",
        leftTableName);
  }

  public String createTempTableName() {
    return "_" + UUID.randomUUID().toString().replace("-", "");
  }
}
