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
package feast.spark.ingestion.delta;

import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static org.apache.spark.sql.functions.*;

import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto.Store.DeltaConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.SparkSink;
import io.delta.tables.DeltaTable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sink for writing row data into Delta Lake. */
public class SparkDeltaSink implements SparkSink {
  private static final Logger log = LoggerFactory.getLogger(SparkDeltaSink.class);

  private final String jobId;
  private final DeltaConfig deltaConfig;
  private final SparkSession spark;

  private final Map<String, FeatureSetSpec> featureSetSpecsByKey;

  public SparkDeltaSink(
      String jobId,
      DeltaConfig deltaConfig,
      SparkSession spark,
      Map<String, FeatureSetSpec> featureSetSpecsByKey) {
    this.jobId = jobId;
    this.deltaConfig = deltaConfig;
    this.spark = spark;
    this.featureSetSpecsByKey = featureSetSpecsByKey;
  }

  public VoidFunction2<Dataset<byte[]>, Long> configure() {
    String deltaPath = deltaConfig.getPath();

    FeatureRowToSparkRow mapper = new FeatureRowToSparkRow(jobId);

    List<FeatureSetInfo> featureSetInfos = new ArrayList<FeatureSetInfo>();

    for (Entry<String, FeatureSetSpec> specEntry : featureSetSpecsByKey.entrySet()) {

      String specKey = specEntry.getKey();
      FeatureSetSpec spec = specEntry.getValue();
      StructType schema = mapper.buildSchema(spec);

      // Initialize Delta table if needed
      String deltaTablePath = getDeltaTablePath(deltaPath, spec);
      String deltaTableName = String.format("%s_%s", spec.getProject(), spec.getName());
      log.info(
          "Saving FeatureSet {} into Table {} at path {} with schema {}",
          specKey,
          deltaTableName,
          deltaTablePath,
          schema);
      spark
          .createDataFrame(Collections.emptyList(), schema)
          .write()
          .format("delta")
          .partitionBy(FeatureRowToSparkRow.EVENT_TIMESTAMP_DAY_COLUMN)
          .mode("append")
          .option("path", deltaTablePath.toString())
          .option("mergeSchema", "true")
          .saveAsTable(deltaTableName);

      featureSetInfos.add(new FeatureSetInfo(specKey, spec, schema, deltaTablePath));
    }

    return new DeltaSinkFunction(featureSetInfos, mapper);
  }

  @SuppressWarnings("serial")
  private static class DeltaSinkFunction implements VoidFunction2<Dataset<byte[]>, Long> {

    private static final String ROW_NUMBER_COL_NAME = "__row_number";
    private final List<FeatureSetInfo> featureSetInfos;
    private final FeatureRowToSparkRow mapper;
    private transient Map<String, DeltaTable> deltaTables = new HashMap<>();

    private DeltaSinkFunction(List<FeatureSetInfo> featureSetInfos, FeatureRowToSparkRow mapper) {
      this.featureSetInfos = featureSetInfos;
      this.mapper = mapper;
    }

    @Override
    public void call(Dataset<byte[]> batchDF, Long batchId) throws Exception {
      log.info("{} entries", featureSetInfos.size());
      for (FeatureSetInfo fsInfo : featureSetInfos) {
        StructType schema = fsInfo.schema;
        deltaTables.putIfAbsent(fsInfo.tablePath, DeltaTable.forPath(fsInfo.tablePath));
        DeltaTable deltaTable = deltaTables.get(fsInfo.tablePath);

        Dataset<Row> rows =
            batchDF.flatMap(
                r -> {
                  FeatureRow featureRow = FeatureRow.parseFrom(r);
                  log.debug("Comparing key '{}' and '{}'", fsInfo.key, featureRow.getFeatureSet());
                  if (!fsInfo.key.equals(featureRow.getFeatureSet())) {
                    return Collections.emptyIterator();
                  }
                  return Collections.singletonList(mapper.apply(fsInfo.spec, featureRow))
                      .iterator();
                },
                RowEncoder.apply(schema));
        if (rows.isEmpty()) {
          log.info("No rows for {}", fsInfo.key);
          return;
        }

        List<String> entityNames =
            fsInfo.spec.getEntitiesList().stream()
                .map(EntitySpec::getName)
                .collect(Collectors.toList());
        upsertToDelta(deltaTable, rows, batchId, entityNames);
      }
    }

    private void upsertToDelta(
        DeltaTable deltaTable,
        Dataset<Row> microBatchOutputDF,
        long batchId,
        List<String> entityNames) {

      Column[] entityCols = entityNames.stream().map(functions::col).toArray(Column[]::new);

      WindowSpec w = Window.partitionBy(entityCols).orderBy(monotonicallyIncreasingId().desc());

      // Deduplicate rows in minibatch, as MERGE throws an exception on duplicates
      Dataset<Row> dedupDF =
          microBatchOutputDF
              .withColumn(ROW_NUMBER_COL_NAME, row_number().over(w))
              .where(col(ROW_NUMBER_COL_NAME).$eq$eq$eq(1))
              .drop(ROW_NUMBER_COL_NAME);

      // Create condition predicate, e.g. "s.key1 = t.key1 and s.key2 = t.key2"
      String condition =
          entityNames.stream()
              .map(n -> String.format("s.%s = t.%s", n, n))
              .collect(Collectors.joining(" and "));

      deltaTable
          .as("t")
          .merge(dedupDF.as("s"), condition)
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute();
    }
  }

  public static String getDeltaTablePath(String deltaPath, FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", deltaPath, getFeatureSetStringRef(featureSetSpec));
  }

  @SuppressWarnings("serial")
  private static class FeatureSetInfo implements Serializable {
    private final String key;
    private final FeatureSetSpec spec;
    private final StructType schema;
    private final String tablePath;

    private FeatureSetInfo(String key, FeatureSetSpec spec, StructType schema, String tablePath) {
      this.key = key;
      this.spec = spec;
      this.schema = schema;
      this.tablePath = tablePath.toString();
    }
  }
}
