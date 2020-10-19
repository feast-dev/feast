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
package feast.spark.ingestion.metrics.sources;

import com.codahale.metrics.MetricRegistry;
import feast.common.models.FeatureSetReference;
import feast.spark.ingestion.metrics.FeatureRowMetrics;
import feast.spark.ingestion.metrics.FeatureValueMetrics;
import feast.spark.ingestion.metrics.gauges.SimpleGauge;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

public class FeastMetricsSource implements Source, Serializable {

  private static final Logger log = LoggerFactory.getLogger(FeastMetricsSource.class);

  private static final String FEAST_METRICS_SOURCE = "FeastMetrics";

  private final Map<MetricTags, SimpleGauge<Long>> deadLetterRowCountGaugeMap = new HashMap<>();
  private final Map<MetricTags, SimpleGauge<Long>> rowCountGaugeMap = new HashMap<>();
  private final Map<MetricTags, FeatureValueMetrics> featureValueMetricsMap = new HashMap<>();
  private final Map<MetricTags, FeatureRowMetrics> featureRowMetricsMap = new HashMap<>();
  private final Map<MetricTags, SimpleGauge<Long>> featureNameMissingCountMap = new HashMap<>();

  private final MetricRegistry metricRegistry;

  public FeastMetricsSource() {
    metricRegistry = new MetricRegistry();
  }

  @Override
  public String sourceName() {
    return FEAST_METRICS_SOURCE;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }

  public SimpleGauge<Long> getRowCountGauge(
      String metricsNamespace, FeatureSetReference featureSetRef, String storeName) {
    return rowCountGaugeMap.get(
        MetricTags.newBuilder()
            .setMetricsNamespace(metricsNamespace)
            .setFeatureSetRef(featureSetRef)
            .setStoreName(storeName)
            .build());
  }

  public Map<MetricTags, SimpleGauge<Long>> getRowCountGaugeMap() {
    return rowCountGaugeMap;
  }

  public static FeastMetricsSource getMetricsSource() {
    Seq<Source> sourcesByName =
        SparkEnv.get().metricsSystem().getSourcesByName(FEAST_METRICS_SOURCE);

    if (sourcesByName.isEmpty()) {
      return null;
    }

    return (FeastMetricsSource) sourcesByName.apply(0);
  }

  public static boolean isMetricsSourceRegistered() {
    Seq<Source> sourcesByName =
        SparkEnv.get().metricsSystem().getSourcesByName(FEAST_METRICS_SOURCE);

    return !sourcesByName.isEmpty();
  }

  public FeatureValueMetrics getFeatureValueMetrics(
      String metricsNamespace,
      FeatureSetReference featureSetRef,
      String featureName,
      String storeName) {
    return featureValueMetricsMap.get(
        MetricTags.newBuilder()
            .setMetricsNamespace(metricsNamespace)
            .setFeatureSetRef(featureSetRef)
            .setFeatureName(featureName)
            .setStoreName(storeName)
            .build());
  }

  public Map<MetricTags, FeatureValueMetrics> getFeatureValueMetricsMap() {
    return featureValueMetricsMap;
  }

  public Map<MetricTags, FeatureRowMetrics> getFeatureRowMetricsMap() {
    return featureRowMetricsMap;
  }

  public FeatureRowMetrics getFeatureRowMetrics(
      String metricsNamespace, FeatureSetReference featureSetRef, String storeName) {
    return featureRowMetricsMap.get(
        MetricTags.newBuilder()
            .setMetricsNamespace(metricsNamespace)
            .setFeatureSetRef(featureSetRef)
            .setStoreName(storeName)
            .build());
  }

  public Map<MetricTags, SimpleGauge<Long>> getFeatureNameMissingCountMap() {
    return featureNameMissingCountMap;
  }

  public SimpleGauge<Long> getDeadLetterRowCountGauge(FeatureSetReference featureSetRef) {
    return deadLetterRowCountGaugeMap.get(
        MetricTags.newBuilder().setFeatureSetRef(featureSetRef).build());
  }

  public Map<MetricTags, SimpleGauge<Long>> getDeadLetterRowCountGaugeMap() {
    return deadLetterRowCountGaugeMap;
  }
}
