package feast.ingestion.transform.metrics;

import com.google.auto.value.AutoValue;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value.ValCase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteRowMetricsDoFn extends DoFn<KV<Integer, Iterable<FeatureRow>>, Void> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteRowMetricsDoFn.class);

  private final String METRIC_PREFIX = "feast_ingestion";
  private final String STORE_TAG_KEY = "feast_store";
  private final String FEATURE_SET_NAME_TAG_KEY = "feast_featureSet_name";
  private final String FEATURE_SET_VERSION_TAG_KEY = "feast_featureSet_version";
  private final String FEATURE_TAG_KEY = "feast_feature_name";
  private final String INGESTION_JOB_NAME_KEY = "ingestion_job_name";

  public abstract String getStoreName();

  public abstract FeatureSetSpec getFeatureSetSpec();

  public abstract String getStatsdHost();

  public abstract int getStatsdPort();

  public StatsDClient statsd;

  public static Builder newBuilder() {
    return new AutoValue_WriteRowMetricsDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStoreName(String storeName);

    public abstract Builder setFeatureSetSpec(FeatureSetSpec featureSetSpec);

    public abstract Builder setStatsdHost(String statsdHost);

    public abstract Builder setStatsdPort(int statsdPort);

    public abstract WriteRowMetricsDoFn build();
  }


  @ProcessElement
  public void processElement(ProcessContext c) {
    statsd = statsd != null ? statsd : new NonBlockingStatsDClient(
        METRIC_PREFIX,
        getStatsdHost(),
        getStatsdPort()
    );

    Long currentTimestamp = System.currentTimeMillis();
    FeatureSetSpec featureSetSpec = getFeatureSetSpec();

    long rowCount = 0;
    long missingValueCount = 0;

    try {
      for (FeatureRow row : c.element().getValue()) {
        rowCount++;
        long eventTimestamp = row.getEventTimestamp().getSeconds() * 1000;
        long lag = currentTimestamp - eventTimestamp;

        statsd.gauge("feature_row_lag_ms", lag, STORE_TAG_KEY + ":" + getStoreName(),
            FEATURE_SET_NAME_TAG_KEY + ":" + featureSetSpec.getName(),
            FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetSpec.getVersion(),
            INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

        for (Field field : row.getFieldsList()) {
          if (!field.getValue().getValCase().equals(ValCase.VAL_NOT_SET)) {
            statsd.gauge("feature_value_lag_ms", lag, STORE_TAG_KEY + ":" + getStoreName(),
                FEATURE_SET_NAME_TAG_KEY + ":" + featureSetSpec.getName(),
                FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetSpec.getVersion(),
                FEATURE_TAG_KEY + ":" + field.getName(),
                INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());
          } else {
            missingValueCount++;
          }
        }
      }

      statsd.count("feature_row_ingested_count", rowCount,
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetSpec.getName(),
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetSpec.getVersion(),
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

      statsd.count("feature_row_missing_value_count", missingValueCount,
          STORE_TAG_KEY + ":" + getStoreName(),
          FEATURE_SET_NAME_TAG_KEY + ":" + featureSetSpec.getName(),
          FEATURE_SET_VERSION_TAG_KEY + ":" + featureSetSpec.getVersion(),
          INGESTION_JOB_NAME_KEY + ":" + c.getPipelineOptions().getJobName());

    } catch (StatsDClientException e) {
      log.warn("Unable to push metrics to server", e);
    }
  }
}
