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
package feast.spark.ingestion.metrics.writers;

import static org.slf4j.LoggerFactory.getLogger;

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.metrics.WriteFeatureValueMetrics;
import feast.spark.ingestion.metrics.WriteRowMetrics;
import feast.spark.ingestion.metrics.gauges.SimpleGauge;
import feast.spark.ingestion.metrics.sources.FeastMetricsSource;
import feast.spark.ingestion.metrics.sources.MetricTags;
import feast.spark.ingestion.metrics.utils.MetricsSourceUtil;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;

public class FeastMetricsWriter implements MetricsWriter<FeatureRow> {

  private static final Logger log = getLogger(FeastMetricsWriter.class);
  public static final String METRICS_NAMESPACE = "Inflight";

  private final String metricsNamespace = METRICS_NAMESPACE;
  private final String jobName;
  private final List<String> storeNames;
  private final Clock clock;

  public FeastMetricsWriter(String jobName, List<String> storeNames, Clock clock) {
    this.jobName = jobName;
    this.storeNames = storeNames;
    this.clock = clock;
  }

  @Override
  public void writeMetrics(
      String storeName,
      Dataset<FeatureRow> input,
      Map<FeatureSetReference, FeatureSetSpec> featureSetSpecs) {
    try {

      JavaPairRDD<FeatureSetReference, Iterable<FeatureRowProto.FeatureRow>> groupedRDD =
          input
              .toJavaRDD()
              .groupBy(featureRow -> FeatureSetReference.parse(featureRow.getFeatureSet()));

      // TODO Rework to avoid use of RDDs
      groupedRDD.foreachPartition(
          tuple2Iterator -> {
            FeastMetricsSource metricsSource = registerMetrics(featureSetSpecs);

            WriteFeatureValueMetrics featureValueMetrics =
                new WriteFeatureValueMetrics(metricsSource);

            WriteRowMetrics rowMetrics = new WriteRowMetrics(metricsSource);

            tuple2Iterator.forEachRemaining(
                tuple -> {
                  featureValueMetrics.processMetric(
                      metricsNamespace, tuple._1, storeName, tuple._2, featureSetSpecs);
                  rowMetrics.processMetric(clock, metricsNamespace, tuple._1, storeName, tuple._2);

                  SimpleGauge<Long> rowCountGauge =
                      metricsSource.getRowCountGauge(metricsNamespace, tuple._1, storeName);
                  for (FeatureRow featureRow : tuple._2) {
                    rowCountGauge.setValue(rowCountGauge.getValue() + 1);
                  }
                });

            SparkEnv.get().metricsSystem().report();
          });

    } catch (Exception t) {
      log.error("Error writing metrics", t);
    }
  }

  private FeastMetricsSource registerMetrics(
      Map<FeatureSetReference, FeatureSetProto.FeatureSetSpec> featureSetSpecs) {
    FeastMetricsSource metricsSource = FeastMetricsSource.getMetricsSource();
    if (metricsSource == null) {
      log.info("Register metrics source on executor");
      MetricsSystem system = SparkEnv.get().metricsSystem();
      metricsSource = new FeastMetricsSource();

      system.registerSource(metricsSource);
      log.info("Registered metrics source on executor");
    }

    Set<FeatureSetReference> meteredFeatureSetRefs =
        metricsSource.getFeatureRowMetricsMap().keySet().stream()
            .map(MetricTags::getFeatureSetRef)
            .collect(Collectors.toSet());
    Map<FeatureSetReference, FeatureSetSpec> additonalFeatureSetSpecs =
        featureSetSpecs.entrySet().stream()
            .filter(s -> !meteredFeatureSetRefs.contains(s.getKey()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    for (Map.Entry<FeatureSetReference, FeatureSetSpec> entry :
        additonalFeatureSetSpecs.entrySet()) {
      log.info("Adding metrics for feature set: {}", entry.getKey());
      addMetrics(metricsSource, entry);
    }

    return metricsSource;
  }

  private void addMetrics(
      FeastMetricsSource metricsSource, Entry<FeatureSetReference, FeatureSetSpec> entry) {
    for (String storeName : storeNames) {
      MetricsSourceUtil.registerFeatureRowCountMetrics(
          metricsNamespace,
          entry.getKey(),
          jobName,
          storeName,
          metricsSource.metricRegistry(),
          metricsSource.getRowCountGaugeMap());

      MetricsSourceUtil.registerFeatureRowMetrics(
          metricsNamespace,
          entry.getKey(),
          jobName,
          storeName,
          metricsSource.metricRegistry(),
          metricsSource.getFeatureRowMetricsMap());
    }

    for (FeatureSetProto.FeatureSpec featureSpec : entry.getValue().getFeaturesList()) {
      for (String storeName : storeNames) {
        MetricsSourceUtil.registerFeatureNameMissingCount(
            metricsNamespace,
            entry.getKey(),
            featureSpec.getName(),
            jobName,
            storeName,
            metricsSource.metricRegistry(),
            metricsSource.getFeatureNameMissingCountMap());
        MetricsSourceUtil.registerFeatureValueMetrics(
            metricsNamespace,
            entry.getKey(),
            featureSpec.getName(),
            jobName,
            storeName,
            metricsSource.metricRegistry(),
            metricsSource.getFeatureValueMetricsMap());
      }
    }
  }
}
