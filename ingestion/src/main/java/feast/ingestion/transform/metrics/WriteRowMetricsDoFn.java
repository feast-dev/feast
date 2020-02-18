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
import feast.types.ValueProto.Value;
import feast.types.ValueProto.Value.ValCase;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteRowMetricsDoFn extends DoFn<FeatureRow, Void> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteRowMetricsDoFn.class);

  private static final String METRIC_PREFIX = "feast_ingestion";
  private static final String STORE_TAG_KEY = "feast_store";
  private static final String FEATURE_SET_PROJECT_TAG_KEY = "feast_project_name";
  private static final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private static final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";
  private static final String FEATURE_NAME_TAG_KEY = "feast_feature_name";
  private static final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";

  public abstract String getStoreName();

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
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

      statsd.histogram(
          "feature_row_event_time_epoch_ms",
          eventTimestamp,
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

      for (Field field : row.getFieldsList()) {
        String[] tags = {
            STORE_TAG_KEY + ":" + getStoreName(),
            FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
            FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
            FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
            FEATURE_NAME_TAG_KEY + ":" + field.getName(),
            INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName()
        };

        if (field.getValue().getValCase().equals(ValCase.VAL_NOT_SET)) {
          statsd.count("feature_value_missing_count", 1, tags);
        } else {
          // Write "numeric" value of the feature.
          // Non numeric feature, like string or byte is ignored.
          // For boolean, true is treated as 1, false as 0.
          // For list, the average numerical value is used.
          writeFeatureValueMetric("feature_value", field, statsd, tags);
          statsd.histogram("feature_value_lag_ms",
              System.currentTimeMillis() - eventTimestamp, tags);
        }
      }

      statsd.count(
          "feature_row_ingested_count",
          1,
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_PROJECT_TAG_KEY + ":" + featureSetProject,
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetName,
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetVersion,
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

    } catch (StatsDClientException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }

  @SuppressWarnings("SameParameterValue")
  private void writeFeatureValueMetric(String metricName, Field field, StatsDClient statsd,
      String[] tags) {
    if (metricName == null || field == null || statsd == null) {
      return;
    }

    Value fieldValue = field.getValue();
    boolean shouldWrite = true;
    double value = 0.0;

    switch (fieldValue.getValCase()) {
      case INT32_VAL:
        value = fieldValue.getInt32Val();
        break;
      case INT64_VAL:
        value = fieldValue.getInt64Val();
        break;
      case DOUBLE_VAL:
        value = fieldValue.getDoubleVal();
        break;
      case FLOAT_VAL:
        value = fieldValue.getFloatVal();
        break;
      case BOOL_VAL:
        value = fieldValue.getBoolVal() ? 1 : 0;
        break;
      case INT32_LIST_VAL:
        List<Integer> int32valList = fieldValue.getInt32ListVal().getValList();
        for (Integer val : int32valList) {
          value += val;
        }
        value = value / int32valList.size();
        break;
      case INT64_LIST_VAL:
        List<Long> int64valList = fieldValue.getInt64ListVal().getValList();
        for (Long val : int64valList) {
          value += val;
        }
        value = value / int64valList.size();
        break;
      case DOUBLE_LIST_VAL:
        List<Double> doubleValList = fieldValue.getDoubleListVal().getValList();
        for (Double val : doubleValList) {
          value += val;
        }
        value = value / doubleValList.size();
        break;
      case FLOAT_LIST_VAL:
        List<Float> floatValList = fieldValue.getFloatListVal().getValList();
        for (Float val : floatValList) {
          value += val;
        }
        value = value / floatValList.size();
        break;
      case BOOL_LIST_VAL:
        List<Boolean> boolValList = fieldValue.getBoolListVal().getValList();
        for (Boolean val : boolValList) {
          if (val) {
            value += 1;
          }
        }
        value = value / boolValList.size();
        break;
      case BYTES_VAL:
      case BYTES_LIST_VAL:
      case STRING_VAL:
      case STRING_LIST_VAL:
      case VAL_NOT_SET:
      default:
        shouldWrite = false;
    }

    if (shouldWrite) {
      if (value < 1) {
        // For negative value, Statsd record it as delta rather than the actual value
        // So we need to "zero" the value first.
        // https://github.com/statsd/statsd/blob/master/docs/metric_types.md#gauges
        statsd.recordGaugeValue(metricName, 0, tags);
      }
      statsd.recordGaugeValue(metricName, value, tags);
    }
  }
}
