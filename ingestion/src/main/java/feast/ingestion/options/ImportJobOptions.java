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
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.options.Validation.Required;

public interface ImportJobOptions extends PipelineOptions {
  @Description("Import spec yaml file path")
  @Required(groups = {"importSpec"})
  String getImportSpecYamlFile();

  void setImportSpecYamlFile(String value);

  @Description("Import spec as native proto binary encoding converted to Base64 string")
  @Required(groups = {"importSpec"})
  String getImportSpecBase64();

  void setImportSpecBase64(String value);

  @Description("If dry run is set, execute up to feature row validation")
  @Default.Boolean(false)
  Boolean isDryRun();

  void setDryRun(Boolean value);

  @Description("Uri of the core api service. eg: localhost:8001")
  @Required(groups = {"coreApi"})
  String getCoreApiUri();

  void setCoreApiUri(String value);

  @Description(
      "Path to root directory of json spec jsonfiles, substitutes for the core api service")
  @Required(groups = {"coreApi"})
  String getCoreApiSpecPath();

  void setCoreApiSpecPath(String value);

  @Override
  @Description("The beam sink class to which the metrics will be pushed")
  @Default.InstanceFactory(NoOpMetricsSink.class)
  Class<? extends MetricsSink> getMetricsSink();

  @Override
  void setMetricsSink(Class<? extends MetricsSink> metricsSink);

  @Description("Filter a to a max number if rows from the read source")
  Long getLimit();

  void setLimit(Long value);

  @Description(
      "Set a store id to store errors in, if your data input is **very** small, you can use STDOUT"
          + " or STDERR as the store id, otherwise it must match an associated storage spec")
  String getErrorsStoreId();

  void setErrorsStoreId(String value);

  @AutoService(PipelineOptionsRegistrar.class)
  class ImportJobOptionsRegistrar implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return Collections.singleton(ImportJobOptions.class);
    }
  }
}
