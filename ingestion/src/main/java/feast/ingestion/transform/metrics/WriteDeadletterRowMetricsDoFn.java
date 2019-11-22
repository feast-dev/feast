package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import feast.ingestion.values.FailedElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteDeadletterRowMetricsDoFn extends DoFn<FailedElement, Void> {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(WriteDeadletterRowMetricsDoFn.class);

  private final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";
  private final String METRIC_PREFIX = "feast_ingestion";
  private final String STORE_TAG_KEY = "feast_store";
  private final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";

  public abstract String getStoreName();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

  public StatsDClient statsd;

  public static WriteDeadletterRowMetricsDoFn.Builder newBuilder() {
    return new AutoValue_WriteDeadletterRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setStatsdHost(String statsdHost);

    public abstract Builder setStatsdPort(int statsdPort);

    public abstract WriteDeadletterRowMetricsDoFn build();
  }

  @Setup
  public void setup() {
    statsd = new NonBlockingStatsDClient(METRIC_PREFIX, getStatsdHost(), getStatsdPort());
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailedElement ignored = c.element();
    try {
      statsd.count(
          "deadletter_row_count",
          1,
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_NAME_TAG_KEY + ":" + ignored.getFeatureSetName(),
          FEATURE_SET_VERSION_TAG_KEY + ":" + ignored.getFeatureSetVersion(),
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());
    } catch (StatsDClientException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }
}
