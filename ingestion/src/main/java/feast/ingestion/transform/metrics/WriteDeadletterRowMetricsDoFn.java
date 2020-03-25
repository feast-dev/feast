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

import static feast.ingestion.transform.metrics.WriteRowMetricsDoFn.METRIC_NAMESPACE_TAG_KEY;

import com.google.auto.value.AutoValue;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import feast.storage.api.write.FailedElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteDeadletterRowMetricsDoFn extends DoFn<FailedElement, Void> {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(WriteDeadletterRowMetricsDoFn.class);

  private final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";
  private final String METRIC_PREFIX = "feast_ingestion";
  private final String STORE_TAG_KEY = "feast_store";
  private final String PROJECT_TAG_KEY = "feast_project_name";
  private final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";

  public abstract String getStoreName();

  public abstract String getMetricNamespace();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

  public StatsDClient statsd;

  public static WriteDeadletterRowMetricsDoFn.Builder newBuilder() {
    return new AutoValue_WriteDeadletterRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setMetricNamespace(String metricNamespace);

    public abstract Builder setStatsdHost(String statsdHost);

    public abstract Builder setStatsdPort(int statsdPort);

    public abstract WriteDeadletterRowMetricsDoFn build();
  }

  @Setup
  public void setup() {
    statsd = new NonBlockingStatsDClient(METRIC_PREFIX, getStatsdHost(), getStatsdPort());
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailedElement ignored = c.element();
    try {
      statsd.count(
          "deadletter_row_count",
          1,
          STORE_TAG_KEY + ":" + getStoreName(),
          METRIC_NAMESPACE_TAG_KEY + ":" + getMetricNamespace(),
          PROJECT_TAG_KEY + ":" + ignored.getProjectName(),
          FEATURE_SET_NAME_TAG_KEY + ":" + ignored.getFeatureSetName(),
          FEATURE_SET_VERSION_TAG_KEY + ":" + ignored.getFeatureSetVersion(),
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());
    } catch (StatsDClientException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }
}
