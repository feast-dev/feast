package feast.ingestion.transform.metrics;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.ingestion.options.ImportOptions;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

public class WriteMetricsTransform extends PTransform<PCollection<FeatureRowExtended>, PDone> {

  private final String storeName;
  private final FeatureSetSpec featureSetSpec;

  private static final long WINDOW_SIZE_SECONDS = 15;

  public WriteMetricsTransform(String storeName, FeatureSetSpec featureSetSpec) {

    this.storeName = storeName;
    this.featureSetSpec = featureSetSpec;
  }

  @Override
  public PDone expand(PCollection<FeatureRowExtended> input) {
    ImportOptions options = input.getPipeline().getOptions()
        .as(ImportOptions.class);
    switch (options.getMetricsExporterType()) {
      case "prometheus":
        input
            .apply("Window records",
                Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE_SECONDS))))
            .apply("Add key", ParDo.of(new DoFn<FeatureRowExtended, KV<Integer, FeatureRow>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                c.output(KV.of(1, c.element().getRow()));
              }
            }))
            .apply("Collect", GroupByKey.create())
            .apply("Write metrics", ParDo
                .of(new WriteMetricsDoFn(options.getJobName(), storeName, featureSetSpec,
                    options.getPrometheusExporterAddress())));
        return PDone.in(input.getPipeline());
      case "none":
      default:
        input.apply("Noop", ParDo.of(new DoFn<FeatureRowExtended, Void>() {
          @ProcessElement
          public void processElement(ProcessContext c) {}
        }));
        return PDone.in(input.getPipeline());
    }
  }
}
