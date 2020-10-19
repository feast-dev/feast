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

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.types.FeatureRowProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto;
import feast.spark.ingestion.metrics.sources.FeastMetricsSource;
import java.io.Serializable;
import java.util.*;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;

@SuppressWarnings("Duplicates")
public class WriteFeatureValueMetrics implements Serializable {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(WriteFeatureValueMetrics.class);

  private final FeastMetricsSource metricsSource;

  public WriteFeatureValueMetrics(FeastMetricsSource metricsSource) {
    this.metricsSource = metricsSource;
  }

  public void processMetric(
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String storeName,
      Iterable<FeatureRow> featureRows,
      Map<FeatureSetReference, FeatureSetSpec> featureSetSpecs) {
    Map<String, FeatureValueSummaryStatistics> featureValueSummaryStatistics =
        calculateFeatureValueSummaryStatistics(featureSetRef, featureRows);

    if (featureValueSummaryStatistics == null) {
      log.info("null feature value summary stats");
      return;
    }

    for (Map.Entry<String, FeatureValueSummaryStatistics> entry :
        featureValueSummaryStatistics.entrySet()) {

      FeatureValueMetrics featureValueMetrics;
      String featureName = entry.getKey();

      featureValueMetrics =
          metricsSource.getFeatureValueMetrics(
              metricsNamespace, featureSetRef, featureName, storeName);

      // Skip metrics for field if it is an entity.

      if (!featureSetSpecs.containsKey(featureSetRef)) {
        log.error("Feature set specs missing feature set ref {}", featureSetRef);
      }

      List<FeatureSetProto.EntitySpec> entitySpecList =
          featureSetSpecs.get(featureSetRef).getEntitiesList();
      boolean isFieldEntity =
          entitySpecList.stream().anyMatch(entitySpec -> entitySpec.getName().equals(featureName));

      if (isFieldEntity) continue;

      updateFeatureValueMetrics(
          metricsNamespace, featureSetRef, entry.getValue(), featureValueMetrics, featureName);
    }
  }

  protected void updateFeatureValueMetrics(
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      FeatureValueSummaryStatistics featureValueSummaryStatistics,
      FeatureValueMetrics featureValueMetrics,
      String featureName) {
    if (featureValueMetrics == null) {
      log.error(
          "Metrics for feature value {} in feature set {} reference not registered.",
          featureName,
          featureSetRef);
      return;
    }

    DoubleSummaryStatistics stats = featureValueSummaryStatistics.getFeatureNameToStats();

    // stats can return non finite values when there is no element
    // or there is an element that is not a number. Metric should only be sent for finite values.
    if (Double.isFinite(stats.getMin())) {
      if (stats.getMin() < 0) {
        // StatsD gauge will assign a delta instead of the actual value, if there is a sign in
        // the value. E.g. if the value is negative, a delta will be assigned. For this reason,
        // the gauge value is set to zero beforehand.
        // https://github.com/statsd/statsd/blob/master/docs/metric_types.md#gauges
        featureValueMetrics.getMinGauge().setValue(0.0);
      } else {
        featureValueMetrics.getMinGauge().setValue(stats.getMin());
      }
    }

    if (Double.isFinite(stats.getMax())) {
      if (stats.getMax() < 0) {
        featureValueMetrics.getMaxGauge().setValue(0.0);
      } else {
        featureValueMetrics.getMaxGauge().setValue(stats.getMax());
      }
    }
    if (Double.isFinite(stats.getAverage())) {
      if (stats.getAverage() < 0) {
        featureValueMetrics.getMeanGauge().setValue(0.0);
      } else {
        featureValueMetrics.getMeanGauge().setValue(stats.getAverage());
      }
    }

    List<Double> valueList = featureValueSummaryStatistics.getFeatureNameToValues();

    if (valueList == null || valueList.size() < 1) {
      return;
    }

    double[] values = new double[valueList.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = valueList.get(i);
    }

    double p25 = new Percentile().evaluate(values, 25);

    if (Double.isFinite(p25)) {
      if (p25 < 0) {
        featureValueMetrics.getP25Gauge().setValue(0.0);
      } else {
        featureValueMetrics.getP25Gauge().setValue(p25);
      }
    }

    double p50 = new Percentile().evaluate(values, 50);

    if (Double.isFinite(p50)) {
      if (p50 < 0) {
        featureValueMetrics.getP50Gauge().setValue(0.0);
      } else {
        featureValueMetrics.getP50Gauge().setValue(p50);
      }
    }

    double p90 = new Percentile().evaluate(values, 90);

    if (Double.isFinite(p90)) {
      if (p90 < 0) {
        featureValueMetrics.getP90Gauge().setValue(0.0);
      } else {
        featureValueMetrics.getP90Gauge().setValue(p90);
      }
    }

    double p95 = new Percentile().evaluate(values, 95);

    if (Double.isFinite(p95)) {
      if (p95 < 0) {
        featureValueMetrics.getP95Gauge().setValue(0.0);
      } else {
        featureValueMetrics.getP95Gauge().setValue(p95);
      }
    }

    double p99 = new Percentile().evaluate(values, 99);

    if (Double.isFinite(p99)) {
      if (p99 < 0) {
        featureValueMetrics.getP99Gauge().setValue(0.0);
      } else {
        featureValueMetrics.getP99Gauge().setValue(p99);
      }
    }
  }

  protected Map<String, FeatureValueSummaryStatistics> calculateFeatureValueSummaryStatistics(
      FeatureSetReference featureSetRef, Iterable<FeatureRowProto.FeatureRow> featureRows) {
    if (featureSetRef == null) {
      log.error(
          "Feature set reference in the feature row is null. Please check the input feature rows from previous steps");
      return null;
    }
    Map<String, FeatureValueSummaryStatistics> featureValueSummaryStatisticsMap = new HashMap<>();

    for (FeatureRowProto.FeatureRow featureRow : featureRows) {
      for (FieldProto.Field field : featureRow.getFieldsList()) {
        updateStats(featureValueSummaryStatisticsMap, field);
      }
    }
    return featureValueSummaryStatisticsMap;
  }

  // Update stats and values array for the feature represented by the field.
  // If the field contains non-numerical or non-boolean value, the stats and values array
  // won't get updated because we are only concerned with numerical value in metrics data.
  // For boolean value, true and false are treated as numerical value of 1 of 0 respectively.
  private static void updateStats(
      Map<String, FeatureValueSummaryStatistics> featureValueSummaryStatistics,
      FieldProto.Field field) {
    if (featureValueSummaryStatistics == null || field == null) {
      return;
    }

    String featureName = field.getName();
    if (!featureValueSummaryStatistics.containsKey(featureName)) {
      featureValueSummaryStatistics.put(
          featureName,
          FeatureValueSummaryStatistics.newBuilder()
              .setFeatureNameToStats(new DoubleSummaryStatistics())
              .setFeatureNameToValues(new ArrayList<>())
              .build());
    }

    ValueProto.Value value = field.getValue();
    DoubleSummaryStatistics stats =
        featureValueSummaryStatistics.get(featureName).getFeatureNameToStats();
    List<Double> values = featureValueSummaryStatistics.get(featureName).getFeatureNameToValues();

    switch (value.getValCase()) {
      case INT32_VAL:
        stats.accept(value.getInt32Val());
        values.add(((double) value.getInt32Val()));
        break;
      case INT64_VAL:
        stats.accept(value.getInt64Val());
        values.add((double) value.getInt64Val());
        break;
      case DOUBLE_VAL:
        stats.accept(value.getDoubleVal());
        values.add(value.getDoubleVal());
        break;
      case FLOAT_VAL:
        stats.accept(value.getFloatVal());
        values.add((double) value.getFloatVal());
        break;
      case BOOL_VAL:
        stats.accept(value.getBoolVal() ? 1 : 0);
        values.add(value.getBoolVal() ? 1d : 0d);
        break;
      case INT32_LIST_VAL:
        for (Integer val : value.getInt32ListVal().getValList()) {
          stats.accept(val);
          values.add(((double) val));
        }
        break;
      case INT64_LIST_VAL:
        for (Long val : value.getInt64ListVal().getValList()) {
          stats.accept(val);
          values.add(((double) val));
        }
        break;
      case DOUBLE_LIST_VAL:
        for (Double val : value.getDoubleListVal().getValList()) {
          stats.accept(val);
          values.add(val);
        }
        break;
      case FLOAT_LIST_VAL:
        for (Float val : value.getFloatListVal().getValList()) {
          stats.accept(val);
          values.add(((double) val));
        }
        break;
      case BOOL_LIST_VAL:
        for (Boolean val : value.getBoolListVal().getValList()) {
          stats.accept(val ? 1 : 0);
          values.add(val ? 1d : 0d);
        }
        break;
      case BYTES_VAL:
      case BYTES_LIST_VAL:
      case STRING_VAL:
      case STRING_LIST_VAL:
      case VAL_NOT_SET:
      default:
    }

    featureValueSummaryStatistics.put(
        featureName,
        FeatureValueSummaryStatistics.newBuilder()
            .setFeatureNameToStats(stats)
            .setFeatureNameToValues(values)
            .build());
  }
}
