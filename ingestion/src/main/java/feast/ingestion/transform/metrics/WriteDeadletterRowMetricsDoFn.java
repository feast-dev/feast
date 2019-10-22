package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.ingestion.transform.metrics.WriteRowMetricsDoFn.Builder;
import feast.ingestion.values.FailedElement;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

@Slf4j
@AutoValue
public abstract class WriteDeadletterRowMetricsDoFn extends
    DoFn<KV<Integer, Iterable<FailedElement>>, Void> {

  private final String STORE_TAG_KEY = "feast_store";
  private final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";

  public abstract String getStoreName();

  public abstract FeatureSetSpec getFeatureSetSpec();

  public abstract String getPgAddress();

  public static WriteDeadletterRowMetricsDoFn.Builder newBuilder() {
    return new AutoValue_WriteDeadletterRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setFeatureSetSpec(FeatureSetSpec featureSetSpec);

    public abstract Builder setPgAddress(String pgAddress);

    public abstract WriteDeadletterRowMetricsDoFn build();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    CollectorRegistry registry = new CollectorRegistry();

    FeatureSetSpec featureSetSpec = getFeatureSetSpec();
    Gauge rowCount = Gauge.build().name("deadletter_count")
        .help("number of rows that were failed to be processed")
        .labelNames(STORE_TAG_KEY, FEATURE_SET_NAME_TAG_KEY, FEATURE_SET_VERSION_TAG_KEY)
        .register(registry);

    rowCount
        .labels(getStoreName(), featureSetSpec.getName(),
            String.valueOf(featureSetSpec.getVersion()));

    for (FailedElement ignored : c.element().getValue()) {
      rowCount.inc();
    }

    try {
      PushGateway pg = new PushGateway(getPgAddress());
      pg.pushAdd(registry, c.getPipelineOptions().getJobName());
    } catch (IOException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }
}
