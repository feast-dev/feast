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
package feast.ingestion.options;

import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Options passed to Beam to influence the job's execution environment */
public interface ImportOptions extends PipelineOptions, DataflowPipelineOptions, DirectOptions {

  @Required
  @Description(
      "JSON string representation of the FeatureSet that the import job will process, in BZip2 binary format."
          + "FeatureSet follows the format in feast.core.FeatureSet proto."
          + "Mutliple FeatureSetSpec can be passed by specifying '--featureSet={...}' multiple times"
          + "The conversion of Proto message to JSON should follow this mapping:"
          + "https://developers.google.com/protocol-buffers/docs/proto3#json"
          + "Please minify and remove all insignificant whitespace such as newline in the JSON string"
          + "to prevent error when parsing the options")
  byte[] getFeatureSetJson();

  void setFeatureSetJson(byte[] featureSetJson);

  @Required
  @Description(
      "JSON string representation of the Store that import job will write FeatureRow to."
          + "Store follows the format in feast.core.Store proto."
          + "Multiple Store can be passed by specifying '--store={...}' multiple times"
          + "The conversion of Proto message to JSON should follow this mapping:"
          + "https://developers.google.com/protocol-buffers/docs/proto3#json"
          + "Please minify and remove all insignificant whitespace such as newline in the JSON string"
          + "to prevent error when parsing the options")
  List<String> getStoreJson();

  void setStoreJson(List<String> storeJson);

  @Description(
      "(Optional) Deadletter elements will be written to this BigQuery table."
          + "Table spec must follow this format PROJECT_ID:DATASET_ID.PROJECT_ID"
          + "The table will be created if not exists.")
  String getDeadLetterTableSpec();

  /**
   * @param deadLetterTableSpec (Optional) BigQuery table for storing elements that failed to be
   *     processed. Table spec must follow this format PROJECT_ID:DATASET_ID.PROJECT_ID
   */
  void setDeadLetterTableSpec(String deadLetterTableSpec);

  @Description("MetricsAccumulator exporter type to instantiate. Supported type: statsd")
  @Default.String("none")
  String getMetricsExporterType();

  void setMetricsExporterType(String metricsExporterType);

  @Description("Host to write the metrics to. Required if the metrics exporter is set to StatsD.")
  @Default.String("localhost")
  String getStatsdHost();

  void setStatsdHost(String StatsdHost);

  @Description(
      "Port on StatsD server to write metrics to. Required if the metrics exporter is set to StatsD.")
  @Default.Integer(8125)
  int getStatsdPort();

  void setStatsdPort(int StatsdPort);

  @Description(
      "Fixed window size in seconds (default 60) to apply before aggregating the numerical value of "
          + "features and exporting the aggregated values as metrics. Refer to "
          + "feast/ingestion/transform/metrics/WriteFeatureValueMetricsDoFn.java"
          + "for the metric nameas and types used.")
  @Default.Integer(60)
  int getWindowSizeInSecForFeatureValueMetric();

  void setWindowSizeInSecForFeatureValueMetric(int seconds);
}
