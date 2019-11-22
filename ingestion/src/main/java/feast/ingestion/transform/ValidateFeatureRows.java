package feast.ingestion.transform;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto;
import feast.ingestion.transform.fn.ValidateFeatureRowDoFn;
import feast.ingestion.values.FailedElement;
import feast.ingestion.values.FeatureSetSpec;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.tuple.Pair;

@AutoValue
public abstract class ValidateFeatureRows
    extends PTransform<PCollection<FeatureRow>, PCollectionTuple> {

  public abstract Map<String, FeatureSetProto.FeatureSetSpec> getFeatureSetSpecs();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ValidateFeatureRows.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSetSpecs(
        Map<String, FeatureSetProto.FeatureSetSpec> featureSetSpec);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract ValidateFeatureRows build();
  }

  @Override
  public PCollectionTuple expand(PCollection<FeatureRow> input) {

    Map<String, FeatureSetSpec> featureSetSpecs =
        getFeatureSetSpecs().entrySet().stream()
            .map(e -> Pair.of(e.getKey(), new FeatureSetSpec(e.getValue())))
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    return input.apply(
        "ValidateFeatureRows",
        ParDo.of(
                ValidateFeatureRowDoFn.newBuilder()
                    .setFeatureSetSpecs(featureSetSpecs)
                    .setSuccessTag(getSuccessTag())
                    .setFailureTag(getFailureTag())
                    .build())
            .withOutputTags(getSuccessTag(), TupleTagList.of(getFailureTag())));
  }
}
