package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.values.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;


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
    ImportOptions options = input.getPipeline().getOptions()
        .as(ImportOptions.class);
    switch (options.getMetricsExporterType()) {
      case "statsd":

        input.get(getFailureTag())
            .apply("WriteDeadletterMetrics", ParDo.of(
                WriteDeadletterRowMetricsDoFn.newBuilder()
                    .setStatsdHost(options.getStatsdHost())
                    .setStatsdPort(options.getStatsdPort())
                    .setStoreName(getStoreName())
                    .build()));

        input.get(getSuccessTag())
            .apply("WriteRowMetrics", ParDo
                .of(WriteRowMetricsDoFn.newBuilder()
                    .setStatsdHost(options.getStatsdHost())
                    .setStatsdPort(options.getStatsdPort())
                    .setStoreName(getStoreName())
                    .build()));

        return PDone.in(input.getPipeline());
      case "none":
      default:
        input.get(getSuccessTag()).apply("Noop",
            ParDo.of(new DoFn<FeatureRow, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
              }
            }));
        return PDone.in(input.getPipeline());
    }
  }
}
