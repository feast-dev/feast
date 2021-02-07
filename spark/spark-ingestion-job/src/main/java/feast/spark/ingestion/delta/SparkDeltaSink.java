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

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto.Store.DeltaConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.SparkSink;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sink for writing row data into Delta Lake. */
public class SparkDeltaSink implements SparkSink {
  private static final Logger log = LoggerFactory.getLogger(SparkDeltaSink.class);

  private final String jobId;
  private final DeltaConfig deltaConfig;

  private final Map<FeatureSetReference, FeatureSetSpec> featureSetSpecsByKey;

  public SparkDeltaSink(
      String jobId,
      DeltaConfig deltaConfig,
      Map<FeatureSetReference, FeatureSetSpec> featureSetSpecsByKey) {
    this.jobId = jobId;
    this.deltaConfig = deltaConfig;
    this.featureSetSpecsByKey = featureSetSpecsByKey;
  }

  public VoidFunction2<Dataset<FeatureRow>, Long> configure() {
    String deltaPath = deltaConfig.getPath();

    FeatureRowToSparkRow mapper = new FeatureRowToSparkRow(jobId);

    List<FeatureSetInfo> featureSetInfos = new ArrayList<>();

    for (Entry<FeatureSetReference, FeatureSetSpec> specEntry : featureSetSpecsByKey.entrySet()) {

      FeatureSetReference specKey = specEntry.getKey();
      FeatureSetSpec spec = specEntry.getValue();
      StructType schema = mapper.buildSchema(spec);

      String deltaTablePath = getDeltaTablePath(deltaPath, spec);
      log.info("Saving FeatureSet {} at path {} with schema {}", specKey, deltaTablePath, schema);

      featureSetInfos.add(new FeatureSetInfo(specKey, spec, schema, deltaTablePath));
    }

    return new DeltaSinkFunction(featureSetInfos, mapper);
  }

  @SuppressWarnings("serial")
  private static class DeltaSinkFunction implements VoidFunction2<Dataset<FeatureRow>, Long> {

    private final List<FeatureSetInfo> featureSetInfos;
    private final FeatureRowToSparkRow mapper;

    private DeltaSinkFunction(List<FeatureSetInfo> featureSetInfos, FeatureRowToSparkRow mapper) {
      this.featureSetInfos = featureSetInfos;
      this.mapper = mapper;
    }

    @Override
    public void call(Dataset<FeatureRow> batchDF, Long batchId) {
      log.info("{} entries", featureSetInfos.size());
      for (FeatureSetInfo fsInfo : featureSetInfos) {
        StructType schema = fsInfo.schema;

        Dataset<Row> rows =
            batchDF.flatMap(
                (FlatMapFunction<FeatureRow, Row>)
                    featureRow -> {
                      if (!fsInfo.key.equals(
                          FeatureSetReference.parse(featureRow.getFeatureSet()))) {
                        return Collections.emptyIterator();
                      }
                      return Collections.singletonList(mapper.apply(fsInfo.spec, featureRow))
                          .iterator();
                    },
                RowEncoder.apply(schema));
        rows.cache();
        if (rows.isEmpty()) {
          log.info("No rows for {}", fsInfo.key);
          continue;
        }

        appendToDelta(rows, fsInfo.tablePath);
      }
    }

    private void appendToDelta(Dataset<Row> microBatchOutputDF, String deltaTablePath) {

      microBatchOutputDF
          .write()
          .format("delta")
          .partitionBy(FeatureRowToSparkRow.EVENT_TIMESTAMP_DAY_COLUMN)
          .mode("append")
          .option("mergeSchema", "true")
          .save(deltaTablePath);
    }
  }

  public static String getDeltaTablePath(String deltaPath, FeatureSetSpec featureSetSpec) {
    return String.format("%s/%s", deltaPath, getFeatureSetStringRef(featureSetSpec));
  }

  @SuppressWarnings("serial")
  private static class FeatureSetInfo implements Serializable {
    private final FeatureSetReference key;
    private final FeatureSetSpec spec;
    private final StructType schema;
    private final String tablePath;

    private FeatureSetInfo(
        FeatureSetReference key, FeatureSetSpec spec, StructType schema, String tablePath) {
      this.key = key;
      this.spec = spec;
      this.schema = schema;
      this.tablePath = tablePath;
    }
  }
}
