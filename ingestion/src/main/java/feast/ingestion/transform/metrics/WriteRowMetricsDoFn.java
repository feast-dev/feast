/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import com.google.auto.value.AutoValue;
import com.google.protobuf.util.Timestamps;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.Value.ValCase;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteRowMetricsDoFn extends DoFn<KV<String, Iterable<FeatureRow>>, Void> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteRowMetricsDoFn.class);

  public static final String METRIC_PREFIX = "feast_ingestion";
  public static final String STORE_TAG_KEY = "feast_store";
  public static final String FEATURE_SET_PROJECT_TAG_KEY = "feast_project_name";
  public static final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  public static final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";
  public static final String FEATURE_TAG_KEY = "feast_feature_name";
  public static final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";

  public static final String GAUGE_NAME_FEATURE_ROW_LAG_MS_MIN = "feature_row_lag_ms_min";
  public static final String GAUGE_NAME_FEATURE_ROW_LAG_MS_MAX = "feature_row_lag_ms_max";
  public static final String GAUGE_NAME_FEATURE_ROW_LAG_MS_MEAN = "feature_row_lag_ms_mean";
  public static final String GAUGE_NAME_FEATURE_ROW_LAG_MS_PERCENTILE_90 =
      "feature_row_lag_ms_percentile_90";
  public static final String GAUGE_NAME_FEATURE_ROW_LAG_MS_PERCENTILE_95 =
      "feature_row_lag_ms_percentile_95";
  public static final String GAUGE_NAME_FEATURE_ROW_LAG_MS_PERCENTILE_99 =
      "feature_row_lag_ms_percentile_99";

  public static final String GAUGE_NAME_FEATURE_VALUE_LAG_MS_MIN = "feature_value_lag_ms_min";
  public static final String GAUGE_NAME_FEATURE_VALUE_LAG_MS_MAX = "feature_value_lag_ms_max";
  public static final String GAUGE_NAME_FEATURE_VALUE_LAG_MS_MEAN = "feature_value_lag_ms_mean";
  public static final String GAUGE_NAME_FEATURE_VALUE_LAG_MS_PERCENTILE_90 =
      "feature_value_lag_ms_percentile_90";
  public static final String GAUGE_NAME_FEATURE_VALUE_LAG_MS_PERCENTILE_95 =
      "feature_value_lag_ms_percentile_95";
  public static final String GAUGE_NAME_FEATURE_VALUE_LAG_MS_PERCENTILE_99 =
      "feature_value_lag_ms_percentile_99";

  public static final String COUNT_NAME_FEATURE_ROW_INGESTED = "feature_row_ingested_count";
  public static final String COUNT_NAME_FEATURE_VALUE_MISSING = "feature_value_missing_count";

  public abstract String getStoreName();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

  @Nullable
  public abstract Clock getClock();

  public static WriteRowMetricsDoFn create(
      String newStoreName, String newStatsdHost, int newStatsdPort) {
    return newBuilder()
        .setStoreName(newStoreName)
        .setStatsdHost(newStatsdHost)
        .setStatsdPort(newStatsdPort)
        .build();
  }

  public StatsDClient statsd;

  public static Builder newBuilder() {
    return new AutoValue_WriteRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setStatsdHost(String statsdHost);

    public abstract Builder setStatsdPort(int statsdPort);

    /**
     * setClock will override the default system clock used to calculate feature row lag.
     *
     * @param clock Clock instance
     */
    public abstract Builder setClock(Clock clock);

    public abstract WriteRowMetricsDoFn build();
  }

  @Setup
  public void setup() {
    // Note that exception may be thrown during StatsD client instantiation but no exception
    // will be thrown when sending metrics (mimicking the UDP protocol behaviour).
    // https://jar-download.com/artifacts/com.datadoghq/java-dogstatsd-client/2.1.1/documentation
    // https://github.com/DataDog/java-dogstatsd-client#unix-domain-socket-support
    try {
      statsd = new NonBlockingStatsDClient(METRIC_PREFIX, getStatsdHost(), getStatsdPort());
    } catch (Exception e) {
      log.error("StatsD client cannot be started: " + e.getMessage());
    }
  }

  @SuppressWarnings("DuplicatedCode")
  @ProcessElement
  public void processElement(
      ProcessContext c, @Element KV<String, Iterable<FeatureRow>> featureSetRefToFeatureRows) {
    if (statsd == null) {
      log.error("StatsD client is null, likely because it encounters an error during setup");
      return;
    }

    String featureSetRef = featureSetRefToFeatureRows.getKey();
    if (featureSetRef == null) {
      log.error(
          "Feature set reference in the feature row is null. Please check the input feature rows from previous steps");
      return;
    }
    String[] slashSplits = featureSetRef.split("/");
    if (slashSplits.length != 2) {
      log.error(
          "Skip writing feature row metrics because the feature set reference '{}' does not"
              + "follow the required format <project>/<feature_set_name>:<version>",
          featureSetRef);
      return;
    }

    String featureSetProject = slashSplits[0];
    String featureSetName = slashSplits[1];

    // featureRowLagStats is stats for feature row lag for feature set "featureSetName"
    DescriptiveStatistics featureRowLagStats = new DescriptiveStatistics();
    // featureNameToLagStats is stats for feature lag for all features in feature set
    // "featureSetName"
    Map<String, DescriptiveStatistics> featureNameToLagStats = new HashMap<>();
    // featureNameToMissingCount is count for "value_not_set" for all features in feature set
    // "featureSetName"
    Map<String, Long> featureNameToMissingCount = new HashMap<>();

    for (FeatureRow featureRow : featureSetRefToFeatureRows.getValue()) {
      long currentTime = getClock() == null ? System.currentTimeMillis() : getClock().millis();
      long featureRowLag = currentTime - Timestamps.toMillis(featureRow.getEventTimestamp());
      featureRowLagStats.addValue(featureRowLag);

      for (Field field : featureRow.getFieldsList()) {
        String featureName = field.getName();
        Value featureValue = field.getValue();
        if (!featureNameToLagStats.containsKey(featureName)) {
          // Ensure map contains the "featureName" key
          featureNameToLagStats.put(featureName, new DescriptiveStatistics());
        }
        if (!featureNameToMissingCount.containsKey(featureName)) {
          // Ensure map contains the "featureName" key
          featureNameToMissingCount.put(featureName, 0L);
        }
        if (featureValue.getValCase().equals(ValCase.VAL_NOT_SET)) {
          featureNameToMissingCount.put(
              featureName, featureNameToMissingCount.get(featureName) + 1);
        } else {
          featureNameToLagStats.get(featureName).addValue(featureRowLag);
        }
      }
    }

    String[] tags = {
      STORE_TAG_KEY + ":" + getStoreName(),
      FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
      FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
      INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName(),
    };

    statsd.count(COUNT_NAME_FEATURE_ROW_INGESTED, featureRowLagStats.getN(), tags);
    // DescriptiveStatistics returns invalid NaN value for getMin(), getMax(), ... when there is no
    // items in the stats.
    if (featureRowLagStats.getN() > 0) {
      statsd.gauge(GAUGE_NAME_FEATURE_ROW_LAG_MS_MIN, featureRowLagStats.getMin(), tags);
      statsd.gauge(GAUGE_NAME_FEATURE_ROW_LAG_MS_MAX, featureRowLagStats.getMax(), tags);
      statsd.gauge(GAUGE_NAME_FEATURE_ROW_LAG_MS_MEAN, featureRowLagStats.getMean(), tags);
      statsd.gauge(
          GAUGE_NAME_FEATURE_ROW_LAG_MS_PERCENTILE_90, featureRowLagStats.getPercentile(90), tags);
      statsd.gauge(
          GAUGE_NAME_FEATURE_ROW_LAG_MS_PERCENTILE_95, featureRowLagStats.getPercentile(95), tags);
      statsd.gauge(
          GAUGE_NAME_FEATURE_ROW_LAG_MS_PERCENTILE_99, featureRowLagStats.getPercentile(99), tags);
    }

    for (Entry<String, DescriptiveStatistics> entry : featureNameToLagStats.entrySet()) {
      String featureName = entry.getKey();
      String[] tagsWithFeatureName = ArrayUtils.add(tags, FEATURE_TAG_KEY + ":" + featureName);
      DescriptiveStatistics stats = entry.getValue();
      if (stats.getN() > 0) {
        statsd.gauge(GAUGE_NAME_FEATURE_VALUE_LAG_MS_MIN, stats.getMin(), tagsWithFeatureName);
        statsd.gauge(GAUGE_NAME_FEATURE_VALUE_LAG_MS_MAX, stats.getMax(), tagsWithFeatureName);
        statsd.gauge(GAUGE_NAME_FEATURE_VALUE_LAG_MS_MEAN, stats.getMean(), tagsWithFeatureName);
        statsd.gauge(
            GAUGE_NAME_FEATURE_VALUE_LAG_MS_PERCENTILE_90,
            stats.getPercentile(90),
            tagsWithFeatureName);
        statsd.gauge(
            GAUGE_NAME_FEATURE_VALUE_LAG_MS_PERCENTILE_95,
            stats.getPercentile(95),
            tagsWithFeatureName);
        statsd.gauge(
            GAUGE_NAME_FEATURE_VALUE_LAG_MS_PERCENTILE_99,
            stats.getPercentile(99),
            tagsWithFeatureName);
      }
      statsd.count(
          COUNT_NAME_FEATURE_VALUE_MISSING,
          featureNameToMissingCount.get(featureName),
          tagsWithFeatureName);
    }
  }
}
