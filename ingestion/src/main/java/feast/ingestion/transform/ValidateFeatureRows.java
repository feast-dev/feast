package feast.ingestion.transform;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.ingestion.transform.fn.ValidateFeatureRowDoFn;
import feast.ingestion.utils.SpecUtil;
import feast.ingestion.values.FailedElement;
import feast.ingestion.values.Field;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

@AutoValue
public abstract class ValidateFeatureRows extends
    PTransform<PCollection<FeatureRow>, PCollectionTuple> {

  public abstract FeatureSetSpec getFeatureSetSpec();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ValidateFeatureRows.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSetSpec(FeatureSetSpec featureSetSpec);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract ValidateFeatureRows build();
  }

  @Override
  public PCollectionTuple expand(PCollection<FeatureRow> input) {
    Map<String, Field> fieldsByName = SpecUtil
        .getFieldByName(getFeatureSetSpec());

    return input.apply("ValidateFeatureRows",
        ParDo.of(ValidateFeatureRowDoFn.newBuilder()
            .setFeatureSetName(getFeatureSetSpec().getName())
            .setFeatureSetVersion(getFeatureSetSpec().getVersion())
            .setFieldByName(fieldsByName)
            .setSuccessTag(getSuccessTag())
            .setFailureTag(getFailureTag())
            .build())
            .withOutputTags(getSuccessTag(), TupleTagList.of(getFailureTag())));
  }
}
