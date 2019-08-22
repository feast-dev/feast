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

import com.google.auto.service.AutoService;
import java.util.Collections;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Default.Boolean;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.options.Validation.Required;

/** Options passed to Beam to influence the job's execution environment */
public interface ImportJobPipelineOptions
    extends PipelineOptions, DataflowPipelineOptions, DirectOptions {
  @Description(
      "URI Path to import job spec yaml file, accepts file:// and gs:// scheme. For file scheme please use absolute path."
          + "For example: 'file:///absolute/path/to/import/job/spec/yaml' OR 'gs://mybucket/import/job/spec/yaml'")
  @Required
  String getImportJobSpecUri();

  void setImportJobSpecUri(String value);

  @Description("Path to a workspace directory containing importJobSpecs.yaml")
  @Required
  String getWorkspace();

  void setWorkspace(String value);

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

  @AutoService(PipelineOptionsRegistrar.class)
  class ImportJobPipelineOptionsRegistrar implements PipelineOptionsRegistrar {

    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return Collections.singleton(ImportJobPipelineOptions.class);
    }
  }
}
