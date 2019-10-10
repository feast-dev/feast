/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.ingestion.options;

import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Default.Boolean;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Options passed to Beam to influence the job's execution environment */
public interface ImportOptions extends PipelineOptions, DataflowPipelineOptions, DirectOptions {
  @Required
  @Description(
      "JSON string representation of the FeatureSetSpec that the import job will process."
          + "FeatureSetSpec follows the format in feast.core.FeatureSet proto."
          + "Mutliple FeatureSetSpec can be passed by specifying '--featureSetSpec={...}' multiple times"
          + "The conversion of Proto message to JSON should follow this mapping:"
          + "https://developers.google.com/protocol-buffers/docs/proto3#json"
          + "Please minify and remove all insignificant whitespace such as newline in the JSON string"
          + "to prevent error when parsing the options")
  List<String> getFeatureSetSpecJson();

  void setFeatureSetSpecJson(List<String> featureSetSpecJson);

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
      "(Optional) Deadletter elements will be written to this table."
          + "Table spec must follow this format: <project_id>:<dataset_id>.<table_id>"
          + "The table will be created if not exists.")
  String getDeadLetterTableSpec();

  void setDeadLetterTableSpec(String deadLetterTableSpec);

  @Description("Limit of rows to sample and output for debugging")
  @Default.Integer(0)
  int getSampleLimit();

  void setSampleLimit(int value);

  @Description(
      "Enable coalesce rows, merges feature rows within a time window to output only the latest value")
  @Default.Boolean(false)
  boolean isCoalesceRowsEnabled();

  void setCoalesceRowsEnabled(boolean value);

  @Description("Delay in seconds to wait for newer values to coalesce on key before emitting")
  @Default.Integer(10)
  int getCoalesceRowsDelaySeconds();

  void setCoalesceRowsDelaySeconds(int value);

  @Description("Time in seconds to retain feature rows to merge with newer records")
  @Default.Integer(30)
  int getCoalesceRowsTimeoutSeconds();

  void setCoalesceRowsTimeoutSeconds(int value);

  @Description("If dry run is set, execute up to feature row validation")
  @Default.Boolean(false)
  Boolean isDryRun();

  void setDryRun(Boolean value);
}
