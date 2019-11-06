package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.ingestion.values.FailedElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteDeadletterRowMetricsDoFn extends
    DoFn<KV<Integer, Iterable<FailedElement>>, Void> {

  private static final Logger log = org.slf4j.LoggerFactory
      .getLogger(WriteDeadletterRowMetricsDoFn.class);

  private final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";
  private final String METRIC_PREFIX = "feast_ingestion";
  private final String STORE_TAG_KEY = "feast_store";
  private final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";

  public abstract String getStoreName();

  public abstract FeatureSetSpec getFeatureSetSpec();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

  public StatsDClient statsd;

  public static WriteDeadletterRowMetricsDoFn.Builder newBuilder() {
    return new AutoValue_WriteDeadletterRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setFeatureSetSpec(FeatureSetSpec featureSetSpec);

    public abstract Builder setStatsdHost(String statsdHost);

    public abstract Builder setStatsdPort(int statsdPort);

    public abstract WriteDeadletterRowMetricsDoFn build();

  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    statsd = statsd != null ? statsd : new NonBlockingStatsDClient(
        METRIC_PREFIX,
        getStatsdHost(),
        getStatsdPort()
    );

    FeatureSetSpec featureSetSpec = getFeatureSetSpec();

    long rowCount = 0;
    for (FailedElement ignored : c.element().getValue()) {
      rowCount++;
    }

    try {
      statsd.count("deadletter_row_count", rowCount,
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetSpec.getName(),
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetSpec.getVersion(),
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());
    } catch (StatsDClientException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }
}
