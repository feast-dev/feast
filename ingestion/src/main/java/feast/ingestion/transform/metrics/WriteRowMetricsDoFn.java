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
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value.ValCase;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteRowMetricsDoFn extends DoFn<FeatureRow, Void> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteRowMetricsDoFn.class);

  public static final String METRIC_PREFIX = "feast_ingestion";
  public static final String METRIC_NAMESPACE_TAG_KEY = "feast_metric_namespace";
  public static final String STORE_TAG_KEY = "feast_store";
  public static final String FEATURE_SET_PROJECT_TAG_KEY = "feast_project_name";
  public static final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  public static final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";
  public static final String FEATURE_TAG_KEY = "feast_feature_name";
  public static final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";

  public abstract String getStoreName();

  public abstract String getMetricNamespace();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

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

    public abstract Builder setMetricNamespace(String metricNamespace);

    public abstract Builder setStatsdHost(String statsdHost);

    public abstract Builder setStatsdPort(int statsdPort);

    public abstract WriteRowMetricsDoFn build();
  }

  @Setup
  public void setup() {
    statsd = new NonBlockingStatsDClient(METRIC_PREFIX, getStatsdHost(), getStatsdPort());
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    try {
      FeatureRow row = c.element();
      long eventTimestamp = com.google.protobuf.util.Timestamps.toMillis(row.getEventTimestamp());

      String[] split = row.getFeatureSet().split(":");
      String featureSetProject = split[0].split("/")[0];
      String featureSetName = split[0].split("/")[1];
      String featureSetVersion = split[1];

      statsd.histogram(
          "feature_row_lag_ms",
          System.currentTimeMillis() - eventTimestamp,
          STORE_TAG_KEY + ":" + getStoreName(),
          METRIC_NAMESPACE_TAG_KEY + ":" + getMetricNamespace(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

      statsd.histogram(
          "feature_row_event_time_epoch_ms",
          eventTimestamp,
          STORE_TAG_KEY + ":" + getStoreName(),
          METRIC_NAMESPACE_TAG_KEY + ":" + getMetricNamespace(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

      for (Field field : row.getFieldsList()) {
        if (!field.getValue().getValCase().equals(ValCase.VAL_NOT_SET)) {
          statsd.histogram(
              "feature_value_lag_ms",
              System.currentTimeMillis() - eventTimestamp,
              STORE_TAG_KEY + ":" + getStoreName(),
              METRIC_NAMESPACE_TAG_KEY + ":" + getMetricNamespace(),
              FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
              FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
              FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
              FEATURE_TAG_KEY + ":" + field.getName(),
              INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());
        } else {
          statsd.count(
              "feature_value_missing_count",
              1,
              STORE_TAG_KEY + ":" + getStoreName(),
              METRIC_NAMESPACE_TAG_KEY + ":" + getMetricNamespace(),
              FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
              FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
              FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
              FEATURE_TAG_KEY + ":" + field.getName(),
              INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());
        }
      }

      statsd.count(
          "feature_row_ingested_count",
          1,
          STORE_TAG_KEY + ":" + getStoreName(),
          METRIC_NAMESPACE_TAG_KEY + ":" + getMetricNamespace(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

    } catch (StatsDClientException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }
}
