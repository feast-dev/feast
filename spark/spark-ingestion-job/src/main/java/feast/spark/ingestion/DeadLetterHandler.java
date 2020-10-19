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
package feast.spark.ingestion;

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.spark.ingestion.common.FailedElement;
import feast.spark.ingestion.metrics.gauges.SimpleGauge;
import feast.spark.ingestion.metrics.sources.FeastMetricsSource;
import feast.spark.ingestion.metrics.sources.MetricTags;
import feast.spark.ingestion.metrics.utils.MetricsSourceUtil;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterHandler implements Serializable {

  private static Logger log = LoggerFactory.getLogger(DeadLetterHandler.class);

  private final String deadLetterPath;
  private final String jobId;
  private final Map<FeatureSetReference, FeatureSetSpec> featureSetSpecs;

  private static final StructType deadLetterType;

  static {
    StructType schema = new StructType();
    schema = schema.add("timestamp", DataTypes.TimestampType, false);
    schema = schema.add("job_name", DataTypes.StringType, true);
    schema = schema.add("transform_name", DataTypes.StringType, true);
    schema = schema.add("payload", DataTypes.StringType, true);
    schema = schema.add("error_message", DataTypes.StringType, true);
    schema = schema.add("stack_trace", DataTypes.StringType, true);
    deadLetterType = schema;
  }

  public DeadLetterHandler(
      String deadLetterPath,
      String jobId,
      Map<FeatureSetReference, FeatureSetSpec> featureSetSpecs) {
    this.deadLetterPath = deadLetterPath;
    this.jobId = jobId;
    this.featureSetSpecs = featureSetSpecs;
  }

  public void storeDeadLetter(Dataset<FailedElement> invalidRows) {
    invalidRows
        .mapPartitions(
            (MapPartitionsFunction<FailedElement, Row>)
                partition -> {
                  FeastMetricsSource metricsSource = registerMetrics(featureSetSpecs);

                  List<Row> rowsDF = new ArrayList<>();
                  partition.forEachRemaining(
                      element -> {
                        Row row =
                            RowFactory.create(
                                new Timestamp(element.getTimestamp().toEpochMilli()),
                                element.getJobName(),
                                element.getTransformName(),
                                element.getPayload(),
                                element.getErrorMessage(),
                                element.getStackTrace());
                        SimpleGauge<Long> deadLetterRowCountGauge =
                            metricsSource.getDeadLetterRowCountGauge(
                                FeatureSetReference.of(
                                    element.getProjectName(), element.getFeatureSetName()));
                        if (deadLetterRowCountGauge != null) {
                          deadLetterRowCountGauge.setValue(deadLetterRowCountGauge.getValue() + 1);
                        }
                        rowsDF.add(row);
                      });
                  SparkEnv.get().metricsSystem().report();
                  return rowsDF.iterator();
                },
            RowEncoder.apply(deadLetterType))
        .write()
        .format("delta")
        .mode("append")
        .save(deadLetterPath);
  }

  private FeastMetricsSource registerMetrics(
      Map<FeatureSetReference, FeatureSetSpec> featureSetSpecs) {
    FeastMetricsSource metricsSource = FeastMetricsSource.getMetricsSource();
    if (metricsSource == null) {
      log.info("Register metrics source on executor");
      MetricsSystem system = SparkEnv.get().metricsSystem();
      metricsSource = new FeastMetricsSource();

      system.registerSource(metricsSource);
      log.info("Registered metrics source on executor");
    }

    Set<FeatureSetReference> meteredFeatureSetRefs =
        metricsSource.getDeadLetterRowCountGaugeMap().keySet().stream()
            .map(MetricTags::getFeatureSetRef)
            .collect(Collectors.toSet());
    Map<FeatureSetReference, FeatureSetSpec> additonalFeatureSetSpecs =
        featureSetSpecs.entrySet().stream()
            .filter(s -> !meteredFeatureSetRefs.contains(s.getKey()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    for (Map.Entry<FeatureSetReference, FeatureSetSpec> entry :
        additonalFeatureSetSpecs.entrySet()) {
      log.info("Adding deadletter metrics for feature set: {}", entry.getKey());

      MetricsSourceUtil.registerDeadletterRowCountMetrics(
          entry.getKey(),
          jobId,
          metricsSource.metricRegistry(),
          metricsSource.getDeadLetterRowCountGaugeMap());
    }

    return metricsSource;
  }
}
