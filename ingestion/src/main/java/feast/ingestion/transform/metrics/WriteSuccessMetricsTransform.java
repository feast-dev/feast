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
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

@AutoValue
public abstract class WriteSuccessMetricsTransform
    extends PTransform<PCollection<FeatureRow>, PDone> {

  public static final String METRIC_NAMESPACE = "WriteToStoreSuccess";
  public static final String ELEMENTS_WRITTEN_METRIC = "elements_written";
  private static final Counter elementsWritten =
      Metrics.counter(METRIC_NAMESPACE, ELEMENTS_WRITTEN_METRIC);

  public abstract String getStoreName();

  public static WriteSuccessMetricsTransform create(String storeName) {
    return new AutoValue_WriteSuccessMetricsTransform(storeName);
  }

  @Override
  public PDone expand(PCollection<FeatureRow> input) {
    ImportOptions options = input.getPipeline().getOptions().as(ImportOptions.class);

    input.apply(
        "IncrementSuccessfulWriteToStoreElementsWrittenCounter",
        MapElements.into(TypeDescriptors.booleans())
            .via(
                (FeatureRow row) -> {
                  elementsWritten.inc();
                  return true;
                }));

    switch (options.getMetricsExporterType()) {
      case "statsd":

        // Fixed window is applied so the metric collector will not be overwhelmed with the metrics
        // data. For validation, only summaries of the values are usually required vs the actual
        // values.
        PCollection<KV<String, Iterable<FeatureRow>>> validRowsGroupedByRef =
            input
                .apply(
                    "FixedWindow",
                    Window.into(
                        FixedWindows.of(
                            Duration.standardSeconds(
                                options.getWindowSizeInSecForFeatureValueMetric()))))
                .apply(
                    "ConvertToKV_FeatureSetRefToFeatureRow",
                    ParDo.of(
                        new DoFn<FeatureRow, KV<String, FeatureRow>>() {
                          @ProcessElement
                          public void processElement(
                              ProcessContext c, @Element FeatureRow featureRow) {
                            c.output(KV.of(featureRow.getFeatureSet(), featureRow));
                          }
                        }))
                .apply("GroupByFeatureSetRef", GroupByKey.create());

        validRowsGroupedByRef.apply(
            "WriteRowMetrics",
            ParDo.of(
                WriteRowMetricsDoFn.newBuilder()
                    .setStatsdHost(options.getStatsdHost())
                    .setStatsdPort(options.getStatsdPort())
                    .setStoreName(getStoreName())
                    .build()));

        validRowsGroupedByRef.apply(
            "WriteFeatureValueMetrics",
            ParDo.of(
                WriteFeatureValueMetricsDoFn.newBuilder()
                    .setStatsdHost(options.getStatsdHost())
                    .setStatsdPort(options.getStatsdPort())
                    .setStoreName(getStoreName())
                    .build()));

        return PDone.in(input.getPipeline());
      case "none":
      default:
        input.apply(
            "Noop",
            ParDo.of(
                new DoFn<FeatureRow, Void>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {}
                }));
        return PDone.in(input.getPipeline());
    }
  }
}
