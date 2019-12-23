package feast.serving.specs;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureReference;
import java.util.List;

@AutoValue
public abstract class FeatureSetRequest {
  public abstract FeatureSetSpec getSpec();

  public abstract ImmutableSet<FeatureReference> getFeatureReferences();

  public static Builder newBuilder() {
    return new AutoValue_FeatureSetRequest.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSpec(FeatureSetSpec spec);

    abstract ImmutableSet.Builder<FeatureReference> featureReferencesBuilder();

    public Builder addAllFeatureReferences(List<FeatureReference> featureReferenceList) {
      featureReferencesBuilder().addAll(featureReferenceList);
      return this;
    }

    public Builder addFeatureReference(FeatureReference featureReference) {
      featureReferencesBuilder().add(featureReference);
      return this;
    }

    public abstract FeatureSetRequest build();
  }
}
