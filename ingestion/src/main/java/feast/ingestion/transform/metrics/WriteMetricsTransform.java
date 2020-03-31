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
import feast.ingestion.values.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

@AutoValue
public abstract class WriteMetricsTransform extends PTransform<PCollectionTuple, PDone> {

  public abstract String getStoreName();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_WriteMetricsTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract WriteMetricsTransform build();
  }

  @Override
  public PDone expand(PCollectionTuple input) {
    ImportOptions options = input.getPipeline().getOptions().as(ImportOptions.class);
    switch (options.getMetricsExporterType()) {
      case "statsd":
        input
            .get(getFailureTag())
            .apply(
                "WriteDeadletterMetrics",
                ParDo.of(
                    WriteDeadletterRowMetricsDoFn.newBuilder()
                        .setStatsdHost(options.getStatsdHost())
                        .setStatsdPort(options.getStatsdPort())
                        .setStoreName(getStoreName())
                        .build()));

        // Fixed window is applied so the metric collector will not be overwhelmed with the metrics
        // data. For validation, only summaries of the values are usually required vs the actual
        // values.
        PCollection<KV<String, Iterable<FeatureRow>>> validRowsGroupedByRef =
            input
                .get(getSuccessTag())
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
        input
            .get(getSuccessTag())
            .apply(
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
