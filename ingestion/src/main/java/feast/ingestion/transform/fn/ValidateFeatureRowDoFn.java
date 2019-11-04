package feast.ingestion.transform.fn;

import com.google.auto.value.AutoValue;
import feast.ingestion.values.FailedElement;
import feast.ingestion.values.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto;
import feast.types.ValueProto.Value.ValCase;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

@AutoValue
public abstract class ValidateFeatureRowDoFn extends DoFn<FeatureRow, FeatureRow> {

  public abstract String getFeatureSetName();

  public abstract int getFeatureSetVersion();

  public abstract Map<String, Field> getFieldByName();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ValidateFeatureRowDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFeatureSetName(String featureSetName);

    public abstract Builder setFeatureSetVersion(int featureSetVersion);

    public abstract Builder setFieldByName(Map<String, Field> fieldByName);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract ValidateFeatureRowDoFn build();
  }


  @ProcessElement
  public void processElement(ProcessContext context) {
    String error = null;
    String featureSetId = String.format("%s:%d", getFeatureSetName(), getFeatureSetVersion());
    FeatureRow featureRow = context.element();
    if (featureRow.getFeatureSet().equals(featureSetId)) {

      for (FieldProto.Field field : featureRow.getFieldsList()) {
        if (!getFieldByName().containsKey(field.getName())) {
          error =
              String.format(
                  "FeatureRow contains field '%s' which do not exists in FeatureSet '%s' version '%d'. Please check the FeatureRow data.",
                  field.getName(), getFeatureSetName(), getFeatureSetVersion());
          break;
        }
        // If value is set in the FeatureRow, make sure the value type matches
        // that defined in FeatureSetSpec
        if (!field.getValue().getValCase().equals(ValCase.VAL_NOT_SET)) {
          int expectedTypeFieldNumber =
              getFieldByName().get(field.getName()).getType().getNumber();
          int actualTypeFieldNumber = field.getValue().getValCase().getNumber();
          if (expectedTypeFieldNumber != actualTypeFieldNumber) {
            error =
                String.format(
                    "FeatureRow contains field '%s' with invalid type '%s'. Feast expects the field type to match that in FeatureSet '%s'. Please check the FeatureRow data.",
                    field.getName(),
                    field.getValue().getValCase(),
                    getFieldByName().get(field.getName()).getType());
            break;
          }
        }
      }
    } else {
      error = String.format(
          "FeatureRow contains invalid feature set id %s. Please check that the feature rows are being published to the correct topic on the feature stream.",
          featureSetId);
    }

    if (error != null) {
        context.output(
            getFailureTag(),
            FailedElement.newBuilder()
                .setTransformName("ValidateFeatureRow")
                .setJobName(context.getPipelineOptions().getJobName())
                .setPayload(featureRow.toString())
                .setErrorMessage(error)
                .build());
      } else {
        context.output(getSuccessTag(), featureRow);
      }
  }
}
