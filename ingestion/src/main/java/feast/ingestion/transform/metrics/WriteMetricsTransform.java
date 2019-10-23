package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto.FeatureSetSpec;
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

  private static final long WINDOW_SIZE_SECONDS = 15;
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteMetricsTransform.class);

  public abstract String getStoreName();

  public abstract FeatureSetSpec getFeatureSetSpec();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_WriteMetricsTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setFeatureSetSpec(FeatureSetSpec featureSetSpec);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract WriteMetricsTransform build();
  }

  @Override
  public PDone expand(PCollectionTuple input) {
    ImportOptions options = input.getPipeline().getOptions()
        .as(ImportOptions.class);
    switch (options.getMetricsExporterType()) {
      case "prometheus":

        input.get(getFailureTag())
            .apply("Window records",
                new WindowRecords<>(WINDOW_SIZE_SECONDS))
            .apply("Write deadletter metrics", ParDo.of(
                WriteDeadletterRowMetricsDoFn.newBuilder()
                    .setFeatureSetSpec(getFeatureSetSpec())
                    .setPgAddress(options.getPrometheusExporterAddress())
                    .setStoreName(getStoreName())
                    .build()));

        input.get(getSuccessTag())
            .apply("Window records",
                new WindowRecords<>(WINDOW_SIZE_SECONDS))
            .apply("Write row metrics", ParDo
                .of(WriteRowMetricsDoFn.newBuilder()
                    .setFeatureSetSpec(getFeatureSetSpec())
                    .setPgAddress(options.getPrometheusExporterAddress())
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
