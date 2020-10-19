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
package feast.spark.ingestion.metrics.utils;

import com.codahale.metrics.MetricRegistry;
import feast.common.models.FeatureSetReference;
import feast.spark.ingestion.metrics.FeatureRowMetrics;
import feast.spark.ingestion.metrics.FeatureValueMetrics;
import feast.spark.ingestion.metrics.gauges.SimpleGauge;
import feast.spark.ingestion.metrics.sources.MetricTags;
import java.util.Map;

public class MetricsSourceUtil {

  private static final String METRICS_NAMESPACE_KEY = "metrics_namespace";
  private static final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";
  private static final String STORE_TAG_KEY = "feast_store";
  private static final String FEATURE_SET_PROJECT_TAG_KEY = "feast_project_name";
  private static final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private static final String FEATURE_TAG_KEY = "feast_feature_name";

  private static final String FEATURE_ROW_INGESTED_COUNT = "feature_row_ingested_count";
  private static final String DEADLETTER_ROW_COUNT = "deadletter_row_count";

  private static final String FEATURE_VALUE_MIN = "feature_value_min";
  private static final String FEATURE_VALUE_MAX = "feature_value_max";
  private static final String FEATURE_VALUE_MEAN = "feature_value_mean";
  private static final String FEATURE_VALUE_P25 = "feature_value_p25";
  private static final String FEATURE_VALUE_P50 = "feature_value_p50";
  private static final String FEATURE_VALUE_P90 = "feature_value_p90";
  private static final String FEATURE_VALUE_P95 = "feature_value_p95";
  private static final String FEATURE_VALUE_P99 = "feature_value_p99";

  private static final String FEATURE_ROW_LAG_MS_MIN = "feature_row_lag_ms_min";
  private static final String FEATURE_ROW_LAG_MS_MAX = "feature_row_lag_ms_max";
  private static final String FEATURE_ROW_LAG_MS_MEAN = "feature_row_lag_ms_mean";

  private static final String FEATURE_NAME_MISSING_COUNT = "feature_name_missing_count";

  public static String appendTags(String appendAfter, String... tags) {
    if (tags == null) {
      return appendAfter;
    }
    if (tags.length % 2 != 0) {
      return appendAfter;
    }
    StringBuilder taggedRes = new StringBuilder(appendAfter + "#");
    for (int i = 0; i < tags.length; i += 2) {
      taggedRes.append(tags[i]).append("=").append(tags[i + 1]).append(",");
    }
    return taggedRes.substring(0, taggedRes.length() - 1);
  }

  public static void registerDeadletterRowCountMetrics(
      FeatureSetReference featureSetRef,
      String jobName,
      MetricRegistry metricRegistry,
      Map<MetricTags, SimpleGauge<Long>> rowCountGaugeMap) {
    SimpleGauge<Long> rowCountGauge = new SimpleGauge<>(0L);
    rowCountGaugeMap.put(
        MetricTags.newBuilder().setFeatureSetRef(featureSetRef).build(), rowCountGauge);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                DEADLETTER_ROW_COUNT,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName())),
        rowCountGauge);
  }

  public static void registerFeatureRowCountMetrics(
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String jobName,
      String storeName,
      MetricRegistry metricRegistry,
      Map<MetricTags, SimpleGauge<Long>> rowCountGaugeMap) {
    SimpleGauge<Long> rowCountGauge = new SimpleGauge<>(0L);
    rowCountGaugeMap.put(
        MetricTags.newBuilder()
            .setMetricsNamespace(metricsNamespace)
            .setFeatureSetRef(featureSetRef)
            .setStoreName(storeName)
            .build(),
        rowCountGauge);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_ROW_INGESTED_COUNT,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                STORE_TAG_KEY,
                storeName)),
        rowCountGauge);
  }

  public static void registerFeatureRowMetrics(
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String jobName,
      String storeName,
      MetricRegistry metricRegistry,
      Map<MetricTags, FeatureRowMetrics> featureRowMetricsMap) {
    SimpleGauge<Double> minGauge = new SimpleGauge<>();
    SimpleGauge<Double> maxGauge = new SimpleGauge<>();
    SimpleGauge<Double> meanGauge = new SimpleGauge<>();

    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_ROW_LAG_MS_MIN,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                STORE_TAG_KEY,
                storeName)),
        minGauge);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_ROW_LAG_MS_MAX,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                STORE_TAG_KEY,
                storeName)),
        maxGauge);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_ROW_LAG_MS_MEAN,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                STORE_TAG_KEY,
                storeName)),
        meanGauge);

    minGauge.setValue(0.0);
    meanGauge.setValue(0.0);
    maxGauge.setValue(0.0);

    FeatureRowMetrics featureRowMetrics =
        FeatureRowMetrics.newBuilder()
            .setMinGauge(minGauge)
            .setMaxGauge(maxGauge)
            .setMeanGauge(meanGauge)
            .build();

    featureRowMetricsMap.put(
        MetricTags.newBuilder()
            .setMetricsNamespace(metricsNamespace)
            .setFeatureSetRef(featureSetRef)
            .setStoreName(storeName)
            .build(),
        featureRowMetrics);
  }

  public static void registerFeatureNameMissingCount(
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String featureName,
      String jobName,
      String storeName,
      MetricRegistry metricRegistry,
      Map<MetricTags, SimpleGauge<Long>> featureNameMissingCountMap) {
    SimpleGauge<Long> missingCountGauge = new SimpleGauge<>();

    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_NAME_MISSING_COUNT,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        missingCountGauge);

    missingCountGauge.setValue(0L);

    featureNameMissingCountMap.put(
        MetricTags.newBuilder()
            .setMetricsNamespace(metricsNamespace)
            .setFeatureSetRef(featureSetRef)
            .setFeatureName(featureName)
            .setStoreName(storeName)
            .build(),
        missingCountGauge);
  }

  public static void registerFeatureValueMetrics(
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String featureName,
      String jobName,
      String storeName,
      MetricRegistry metricRegistry,
      Map<MetricTags, FeatureValueMetrics> featureValueMetricsMap) {

    SimpleGauge<Double> minGauge = new SimpleGauge<>();
    SimpleGauge<Double> maxGauge = new SimpleGauge<>();
    SimpleGauge<Double> meanGauge = new SimpleGauge<>();
    SimpleGauge<Double> p25 = new SimpleGauge<>();
    SimpleGauge<Double> p50 = new SimpleGauge<>();
    SimpleGauge<Double> p90 = new SimpleGauge<>();
    SimpleGauge<Double> p95 = new SimpleGauge<>();
    SimpleGauge<Double> p99 = new SimpleGauge<>();

    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_MIN,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        minGauge);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_MAX,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        maxGauge);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_MEAN,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        meanGauge);

    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_P25,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        p25);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_P50,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        p50);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_P90,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        p90);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_P95,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        p95);
    metricRegistry.register(
        MetricRegistry.name(
            appendTags(
                FEATURE_VALUE_P99,
                METRICS_NAMESPACE_KEY,
                metricsNamespace,
                INGESTION_JOB_NAME_KEY,
                jobName,
                FEATURE_SET_PROJECT_TAG_KEY,
                featureSetRef.getProjectName(),
                FEATURE_SET_NAME_TAG_KEY,
                featureSetRef.getFeatureSetName(),
                FEATURE_TAG_KEY,
                featureName,
                STORE_TAG_KEY,
                storeName)),
        p99);

    minGauge.setValue(0.0);
    meanGauge.setValue(0.0);
    maxGauge.setValue(0.0);
    p25.setValue(0.0);
    p50.setValue(0.0);
    p90.setValue(0.0);
    p95.setValue(0.0);
    p99.setValue(0.0);

    FeatureValueMetrics featureValueMetrics =
        FeatureValueMetrics.newBuilder()
            .setMinGauge(minGauge)
            .setMaxGauge(maxGauge)
            .setMeanGauge(meanGauge)
            .setP25Gauge(p25)
            .setP50Gauge(p50)
            .setP90Gauge(p90)
            .setP95Gauge(p95)
            .setP99Gauge(p99)
            .build();

    featureValueMetricsMap.put(
        MetricTags.newBuilder()
            .setMetricsNamespace(metricsNamespace)
            .setFeatureSetRef(featureSetRef)
            .setFeatureName(featureName)
            .setStoreName(storeName)
            .build(),
        featureValueMetrics);
  }
}
