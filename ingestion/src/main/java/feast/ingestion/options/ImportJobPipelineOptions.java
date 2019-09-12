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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Options passed to Beam to influence the job's execution environment */
public interface ImportJobPipelineOptions extends PipelineOptions {

  @Description("Path to a workspace directory containing importJobSpecs.yaml")
  @Required
  String getWorkspace();

  void setWorkspace(String value);

  @Description("If dry run is set, execute up to feature row validation")
  @Default.Boolean(false)
  boolean isDryRun();

  void setDryRun(boolean value);

  @AutoService(PipelineOptionsRegistrar.class)
  class ImportJobPipelineOptionsRegistrar implements PipelineOptionsRegistrar {

    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return Collections.singleton(ImportJobPipelineOptions.class);
    }
  }
}
