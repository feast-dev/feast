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
import feast.ingestion.options.ImportOptions;
import feast.storage.api.write.FailedElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@AutoValue
public abstract class WriteFailureMetricsTransform
    extends PTransform<PCollection<FailedElement>, PDone> {

  private static final String METRIC_NAMESPACE = "WriteToStoreFailure";

  public abstract String getStoreName();

  public static WriteFailureMetricsTransform create(String storeName) {
    return new AutoValue_WriteFailureMetricsTransform(storeName);
  }

  @Override
  public PDone expand(PCollection<FailedElement> input) {
    ImportOptions options = input.getPipeline().getOptions().as(ImportOptions.class);
    if ("statsd".equals(options.getMetricsExporterType())) {
      input.apply(
          "WriteDeadletterMetrics",
          ParDo.of(
              WriteDeadletterRowMetricsDoFn.newBuilder()
                  .setMetricNamespace(METRIC_NAMESPACE)
                  .setStatsdHost(options.getStatsdHost())
                  .setStatsdPort(options.getStatsdPort())
                  .setStoreName(getStoreName())
                  .build()));
    }
    return PDone.in(input.getPipeline());
  }
}
