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
package feast.spark.ingestion.metrics;

import com.google.protobuf.util.Timestamps;
import feast.common.models.FeatureSetReference;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.spark.ingestion.metrics.gauges.SimpleGauge;
import feast.spark.ingestion.metrics.sources.FeastMetricsSource;
import feast.spark.ingestion.metrics.sources.MetricTags;
import java.io.Serializable;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;

public class WriteRowMetrics implements Serializable {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteRowMetrics.class);

  private final FeastMetricsSource metricsSource;

  public WriteRowMetrics(FeastMetricsSource metricsSource) {
    this.metricsSource = metricsSource;
  }

  public void processMetric(
      Clock clock,
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String storeName,
      Iterable<FeatureRowProto.FeatureRow> featureRows) {

    DescriptiveStatistics featureRowLagStats =
        calculateFeatureRowMetrics(
            clock,
            metricsNamespace,
            featureSetRef,
            storeName,
            featureRows,
            metricsSource.getFeatureNameMissingCountMap());
    if (featureRowLagStats == null) return;

    FeatureRowMetrics featureRowMetrics =
        metricsSource.getFeatureRowMetrics(metricsNamespace, featureSetRef, storeName);

    if (featureRowMetrics == null) {
      log.error(
          "Metrics for feature rows in feature set {} reference not registered.", featureSetRef);
      return;
    }

    // DescriptiveStatistics returns invalid NaN value for getMin(), getMax(), ... when there is no
    // items in the stats.
    if (featureRowLagStats.getN() > 0) {
      featureRowMetrics.getMinGauge().setValue(featureRowLagStats.getMin());
      featureRowMetrics.getMaxGauge().setValue(featureRowLagStats.getMax());
      featureRowMetrics.getMeanGauge().setValue(featureRowLagStats.getMean());
    }
  }

  protected static DescriptiveStatistics calculateFeatureRowMetrics(
      Clock clock,
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String storeName,
      Iterable<FeatureRowProto.FeatureRow> featureRows,
      Map<MetricTags, SimpleGauge<Long>> featureNameToMissingCountMap) {
    if (featureSetRef == null) {
      log.error(
          "Feature set reference in the feature row is null. Please check the input feature rows from previous steps");
      return null;
    }

    // featureRowLagStats is stats for feature row lag for feature set "featureSetName"
    DescriptiveStatistics featureRowLagStats = new DescriptiveStatistics();
    // featureNameToLagStats is stats for feature lag for all features in feature set
    // "featureSetName"
    Map<String, DescriptiveStatistics> featureNameToLagStats = new HashMap<>();
    // featureNameToMissingCount is count for "value_not_set" for all features in feature set
    // "featureSetName"

    for (FeatureRowProto.FeatureRow featureRow : featureRows) {
      long currentTime = clock == null ? System.currentTimeMillis() : clock.millis();
      long featureRowLag = currentTime - Timestamps.toMillis(featureRow.getEventTimestamp());
      featureRowLagStats.addValue(featureRowLag);

      for (FieldProto.Field field : featureRow.getFieldsList()) {
        String featureName = field.getName();
        ValueProto.Value featureValue = field.getValue();

        MetricTags featureNameMissingCountKey =
            MetricTags.newBuilder()
                .setMetricsNamespace(metricsNamespace)
                .setFeatureSetRef(featureSetRef)
                .setFeatureName(featureName)
                .setStoreName(storeName)
                .build();

        if (!featureNameToLagStats.containsKey(featureName)) {
          // Ensure map contains the "featureName" key
          featureNameToLagStats.put(featureName, new DescriptiveStatistics());
        }

        if (featureValue.getValCase().equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
          if (!featureNameToMissingCountMap.containsKey(featureNameMissingCountKey)) {
            // Skip if feature set / feature name not found in missing count map as metrics are
            // registered at start of query.
            log.error(
                "Feature name {} in feature set {} not registered for missing count metric.",
                featureName,
                featureSetRef);
            continue;
          }

          SimpleGauge<Long> featureNameMissingCountKeyGauge =
              featureNameToMissingCountMap.get(featureNameMissingCountKey);
          featureNameMissingCountKeyGauge.setValue(featureNameMissingCountKeyGauge.getValue() + 1);

        } else {
          featureNameToLagStats.get(featureName).addValue(featureRowLag);
        }
      }
    }
    return featureRowLagStats;
  }
}
