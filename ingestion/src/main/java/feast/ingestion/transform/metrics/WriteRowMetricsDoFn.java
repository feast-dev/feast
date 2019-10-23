package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value.ValCase;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteRowMetricsDoFn extends DoFn<KV<Integer, Iterable<FeatureRow>>, Void> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteRowMetricsDoFn.class);
  private final String STORE_TAG_KEY = "feast_store";
  private final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";
  private final String FEATURE_TAG_KEY = "feast_feature_name";

  public abstract String getStoreName();

  public abstract FeatureSetSpec getFeatureSetSpec();

  public abstract String getPgAddress();

  public static Builder newBuilder() {
    return new AutoValue_WriteRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setFeatureSetSpec(FeatureSetSpec featureSetSpec);

    public abstract Builder setPgAddress(String pgAddress);

    public abstract WriteRowMetricsDoFn build();
  }


  @ProcessElement
  public void processElement(ProcessContext c) {
    CollectorRegistry registry = new CollectorRegistry();

    Summary rowLag = Summary.build().name("row_lag_millis")
        .help("delta between processing and event timestamps in millis")
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.01)
        .quantile(0.99, 0.001)
        .labelNames(STORE_TAG_KEY, FEATURE_SET_NAME_TAG_KEY, FEATURE_SET_VERSION_TAG_KEY)
        .register(registry);
    Summary featureLag = Summary.build().name("feature_lag_millis")
        .help("delta between processing and event timestamps in millis")
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.01)
        .quantile(0.99, 0.001)
        .labelNames(STORE_TAG_KEY, FEATURE_SET_NAME_TAG_KEY, FEATURE_SET_VERSION_TAG_KEY,
            FEATURE_TAG_KEY)
        .register(registry);

    Long currentTimestamp = System.currentTimeMillis();
    FeatureSetSpec featureSetSpec = getFeatureSetSpec();

    for (FeatureRow row : c.element().getValue()) {
      long eventTimestamp = row.getEventTimestamp().getSeconds() * 1000;
      long lag = currentTimestamp - eventTimestamp;
      rowLag
          .labels(getStoreName(), featureSetSpec.getName(), String.valueOf(featureSetSpec.getVersion()))
          .observe(lag);
      for (Field field : row.getFieldsList()) {
        if (!field.getValue().getValCase().equals(ValCase.VAL_NOT_SET)) {
          featureLag
              .labels(getStoreName(), featureSetSpec.getName(),
                  String.valueOf(featureSetSpec.getVersion()), field.getName())
              .observe(lag);
        }
      }
    }

    try {
      PushGateway pg = new PushGateway(getPgAddress());
      pg.pushAdd(registry, c.getPipelineOptions().getJobName());
    } catch (IOException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }
}
