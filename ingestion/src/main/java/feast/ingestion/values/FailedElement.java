package feast.ingestion.values;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

@AutoValue
// Use DefaultSchema annotation so this AutoValue class can be serialized by Beam
// https://issues.apache.org/jira/browse/BEAM-1891
// https://github.com/apache/beam/pull/7334
@DefaultSchema(AutoValueSchema.class)
public abstract class FailedElement {
  public abstract Instant getTimestamp();

  @Nullable
  public abstract String getJobName();

  @Nullable
  public abstract String getFeatureSetName();

  @Nullable
  public abstract String getFeatureSetVersion();

  @Nullable
  public abstract String getTransformName();

  @Nullable
  public abstract String getPayload();

  @Nullable
  public abstract String getErrorMessage();

  @Nullable
  public abstract String getStackTrace();

  public static Builder newBuilder() {
    return new AutoValue_FailedElement.Builder().setTimestamp(Instant.now());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTimestamp(Instant timestamp);

    public abstract Builder setFeatureSetName(String featureSetName);

    public abstract Builder setFeatureSetVersion(String featureSetVersion);

    public abstract Builder setJobName(String jobName);

    public abstract Builder setTransformName(String transformName);

    public abstract Builder setPayload(String payload);

    public abstract Builder setErrorMessage(String errorMessage);

    public abstract Builder setStackTrace(String stackTrace);

    public abstract FailedElement build();
  }
}
