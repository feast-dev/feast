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
package feast.ingestion.transform.metrics;

import static feast.ingestion.transform.metrics.WriteRowMetricsDoFn.*;

import com.google.auto.value.AutoValue;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;

/**
 * WriteFeatureValueMetricsDoFn accepts key value of FeatureSetRef(str) to FeatureRow(List) and
 * writes a histogram of the numerical values of each feature to StatsD.
 *
 * <p>The histogram of the numerical values is represented as the following in StatsD:
 *
 * <ul>
 *   <li>gauge of feature_value_min
 *   <li>gauge of feature_value_max
 *   <li>gauge of feature_value_mean
 *   <li>gauge of feature_value_percentile_50
 *   <li>gauge of feature_value_percentile_90
 *   <li>gauge of feature_value_percentile_95
 * </ul>
 *
 * <p>StatsD timing/histogram metric type is not used since it does not support negative values.
 */
@AutoValue
public abstract class WriteFeatureValueMetricsDoFn
    extends DoFn<KV<String, Iterable<FeatureRow>>, Void> {

  abstract String getStoreName();

  abstract String getMetricNamespace();

  abstract String getStatsdHost();

  abstract int getStatsdPort();

  static Builder newBuilder() {
    return new AutoValue_WriteFeatureValueMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setStoreName(String storeName);

    abstract Builder setMetricNamespace(String metricNamespace);

    abstract Builder setStatsdHost(String statsdHost);

    abstract Builder setStatsdPort(int statsdPort);

    abstract WriteFeatureValueMetricsDoFn build();
  }

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(WriteFeatureValueMetricsDoFn.class);
  private StatsDClient statsDClient;
  public static String GAUGE_NAME_FEATURE_VALUE_MIN = "feature_value_min";
  public static String GAUGE_NAME_FEATURE_VALUE_MAX = "feature_value_max";
  public static String GAUGE_NAME_FEATURE_VALUE_MEAN = "feature_value_mean";
  public static String GAUGE_NAME_FEATURE_VALUE_PERCENTILE_25 = "feature_value_percentile_25";
  public static String GAUGE_NAME_FEATURE_VALUE_PERCENTILE_50 = "feature_value_percentile_50";
  public static String GAUGE_NAME_FEATURE_VALUE_PERCENTILE_90 = "feature_value_percentile_90";
  public static String GAUGE_NAME_FEATURE_VALUE_PERCENTILE_95 = "feature_value_percentile_95";
  public static String GAUGE_NAME_FEATURE_VALUE_PERCENTILE_99 = "feature_value_percentile_99";

  @Setup
  public void setup() {
    // Note that exception may be thrown during StatsD client instantiation but no exception
    // will be thrown when sending metrics (mimicking the UDP protocol behaviour).
    // https://jar-download.com/artifacts/com.datadoghq/java-dogstatsd-client/2.1.1/documentation
    // https://github.com/DataDog/java-dogstatsd-client#unix-domain-socket-support
    try {
      statsDClient = new NonBlockingStatsDClient(METRIC_PREFIX, getStatsdHost(), getStatsdPort());
    } catch (Exception e) {
      log.error("StatsD client cannot be started: " + e.getMessage());
    }
  }

  @Teardown
  public void tearDown() {
    if (statsDClient != null) {
      statsDClient.close();
    }
  }

  @ProcessElement
  public void processElement(
      ProcessContext context,
      @Element KV<String, Iterable<FeatureRow>> featureSetRefToFeatureRows) {
    if (statsDClient == null) {
      return;
    }

    String featureSetRef = featureSetRefToFeatureRows.getKey();
    if (featureSetRef == null) {
      return;
    }
    String[] colonSplits = featureSetRef.split(":");
    if (colonSplits.length != 2) {
      log.error(
          "Skip writing feature value metrics because the feature set reference '{}' does not"
              + "follow the required format <project>/<feature_set_name>:<version>",
          featureSetRef);
      return;
    }
    String[] slashSplits = colonSplits[0].split("/");
    if (slashSplits.length != 2) {
      log.error(
          "Skip writing feature value metrics because the feature set reference '{}' does not"
              + "follow the required format <project>/<feature_set_name>:<version>",
          featureSetRef);
      return;
    }
    String projectName = slashSplits[0];
    String featureSetName = slashSplits[1];
    String version = colonSplits[1];

    Map<String, DoubleSummaryStatistics> featureNameToStats = new HashMap<>();
    Map<String, List<Double>> featureNameToValues = new HashMap<>();
    for (FeatureRow featureRow : featureSetRefToFeatureRows.getValue()) {
      for (Field field : featureRow.getFieldsList()) {
        updateStats(featureNameToStats, featureNameToValues, field);
      }
    }

    for (Entry<String, DoubleSummaryStatistics> entry : featureNameToStats.entrySet()) {
      String featureName = entry.getKey();
      DoubleSummaryStatistics stats = entry.getValue();
      String[] tags = {
        STORE_TAG_KEY + ":" + getStoreName(),
        METRIC_NAMESPACE_TAG_KEY + ":" + getMetricNamespace(),
        FEATURE_SET_PROJECT_TAG_KEY + ":" + projectName,
        FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
        FEATURE_SET_VERSION_TAG_KEY + ":" + version,
        FEATURE_TAG_KEY + ":" + featureName,
        INGESTION_JOB_NAME_KEY + ":" + context.getPipelineOptions().getJobName()
      };

      // stats can return non finite values when there is no element
      // or there is an element that is not a number. Metric should only be sent for finite values.
      if (Double.isFinite(stats.getMin())) {
        if (stats.getMin() < 0) {
          // StatsD gauge will asssign a delta instead of the actual value, if there is a sign in
          // the value. E.g. if the value is negative, a delta will be assigned. For this reason,
          // the gauge value is set to zero beforehand.
          // https://github.com/statsd/statsd/blob/master/docs/metric_types.md#gauges
          statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_MIN, 0, tags);
        }
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_MIN, stats.getMin(), tags);
      }
      if (Double.isFinite(stats.getMax())) {
        if (stats.getMax() < 0) {
          statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_MAX, 0, tags);
        }
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_MAX, stats.getMax(), tags);
      }
      if (Double.isFinite(stats.getAverage())) {
        if (stats.getAverage() < 0) {
          statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_MEAN, 0, tags);
        }
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_MEAN, stats.getAverage(), tags);
      }

      // For percentile calculation, Percentile class from commons-math3 from Apache is used.
      // Percentile requires double[], hence the conversion below.
      if (!featureNameToValues.containsKey(featureName)) {
        continue;
      }
      List<Double> valueList = featureNameToValues.get(featureName);
      if (valueList == null || valueList.size() < 1) {
        continue;
      }
      double[] values = new double[valueList.size()];
      for (int i = 0; i < values.length; i++) {
        values[i] = valueList.get(i);
      }

      double p25 = new Percentile().evaluate(values, 25);
      if (p25 < 0) {
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_25, 0, tags);
      }
      statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_25, p25, tags);

      double p50 = new Percentile().evaluate(values, 50);
      if (p50 < 0) {
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_50, 0, tags);
      }
      statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_50, p50, tags);

      double p90 = new Percentile().evaluate(values, 90);
      if (p90 < 0) {
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_90, 0, tags);
      }
      statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_90, p90, tags);

      double p95 = new Percentile().evaluate(values, 95);
      if (p95 < 0) {
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_95, 0, tags);
      }
      statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_95, p95, tags);

      double p99 = new Percentile().evaluate(values, 99);
      if (p99 < 0) {
        statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_99, 0, tags);
      }
      statsDClient.gauge(GAUGE_NAME_FEATURE_VALUE_PERCENTILE_99, p99, tags);
    }
  }

  // Update stats and values array for the feature represented by the field.
  // If the field contains non-numerical or non-boolean value, the stats and values array
  // won't get updated because we are only concerned with numerical value in metrics data.
  // For boolean value, true and false are treated as numerical value of 1 of 0 respectively.
  private void updateStats(
      Map<String, DoubleSummaryStatistics> featureNameToStats,
      Map<String, List<Double>> featureNameToValues,
      Field field) {
    if (featureNameToStats == null || featureNameToValues == null || field == null) {
      return;
    }

    String featureName = field.getName();
    if (!featureNameToStats.containsKey(featureName)) {
      featureNameToStats.put(featureName, new DoubleSummaryStatistics());
    }
    if (!featureNameToValues.containsKey(featureName)) {
      featureNameToValues.put(featureName, new ArrayList<>());
    }

    Value value = field.getValue();
    DoubleSummaryStatistics stats = featureNameToStats.get(featureName);
    List<Double> values = featureNameToValues.get(featureName);

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
  }
}
