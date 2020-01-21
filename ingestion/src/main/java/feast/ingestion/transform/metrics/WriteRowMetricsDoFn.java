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
import com.timgroup.statsd.StatsDClientException;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteRowMetricsDoFn extends DoFn<FeatureRow, Void> {

  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(WriteRowMetricsDoFn.class);

  private static final String METRIC_PREFIX = "feast_ingestion";
  private static final String STORE_TAG_KEY = "feast_store";
  private static final String FEATURE_SET_PROJECT_TAG_KEY = "feast_project_name";
  private static final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private static final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";
  private static final String FEATURE_TAG_KEY = "feast_feature_name";
  private static final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";

  public abstract String getStoreName();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

  public static WriteRowMetricsDoFn create(String newStoreName, String newStatsdHost,
      int newStatsdPort) {
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

    public abstract WriteRowMetricsDoFn build();
  }

  @Setup
  public void setup() {
    try {
      statsd = new NonBlockingStatsDClient(METRIC_PREFIX, getStatsdHost(), getStatsdPort());
    } catch (StatsDClientException e) {
      LOG.warn("Failed to create StatsD client");
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    if (statsd == null) {
      LOG.warn(
          "No StatsD client available (maybe it failed to initialize). No FeatureRow metrics will be sent.");
      return;
    }

    FeatureRow row = c.element();
    String[] colonSplits = row.getFeatureSet().split(":");
    if (colonSplits.length < 1) {
      LOG.warn(
          "FeatureRow an invalid FeatureSet reference: " + row.getFeatureSet()
              + ". Expected format: PROJECT/FEATURE_SET:VERSION");
      return;
    }
    String[] slashSplits = colonSplits[0].split("/");
    if (slashSplits.length < 2) {
      LOG.warn(
          "FeatureRow an invalid FeatureSet reference: " + row.getFeatureSet()
              + ". Expected format: PROJECT/FEATURE_SET:VERSION");
      return;
    }

    String featureSetProject = slashSplits[0];
    String featureSetName = slashSplits[1];
    String featureSetVersion = colonSplits[1];
    String[] tags = new String[]{
        STORE_TAG_KEY + ":" + getStoreName(),
        FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
        FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
        FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
        INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName()
    };
    long eventTimestamp = Timestamps.toMillis(row.getEventTimestamp());

    statsd.histogram("feature_row_lag_ms", System.currentTimeMillis() - eventTimestamp, tags);
    statsd.histogram("feature_row_event_time_epoch_ms", eventTimestamp, tags);
    statsd.count("feature_row_ingested_count", 1, tags);

    // Feature value metrics will be used for validation
    for (Field field : row.getFieldsList()) {
      tags = new String[]{
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          FEATURE_TAG_KEY + ":" + field.getName(),
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName()
      };

      Value val = field.getValue();
      switch (val.getValCase()) {
        case INT32_VAL:
          statsd.histogram("feature_value", val.getInt32Val(), tags);
          break;
        case INT64_VAL:
          statsd.histogram("feature_value", val.getInt64Val(), tags);
          break;
        case DOUBLE_VAL:
          statsd.histogram("feature_value", val.getDoubleVal(), tags);
          break;
        case FLOAT_VAL:
          statsd.histogram("feature_value", val.getFloatVal(), tags);
          break;
        case BOOL_VAL:
          statsd.histogram("feature_value", val.getBoolVal() ? 1 : 0, tags);
          break;
        case BYTES_VAL:
        case STRING_VAL:
        case BYTES_LIST_VAL:
        case FLOAT_LIST_VAL:
        case STRING_LIST_VAL:
        case INT32_LIST_VAL:
        case INT64_LIST_VAL:
        case DOUBLE_LIST_VAL:
        case BOOL_LIST_VAL:
          break;
        case VAL_NOT_SET:
          statsd.count("feature_value_missing_count", 1, tags);
          break;
      }
    }
  }
}
