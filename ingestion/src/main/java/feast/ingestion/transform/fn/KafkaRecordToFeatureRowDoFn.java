package feast.ingestion.transform.fn;

import com.google.auto.value.AutoValue;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.ingestion.values.FailedElement;
import feast.ingestion.values.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto;
import feast.types.ValueProto.Value.ValCase;
import java.util.Base64;
import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.exception.ExceptionUtils;

@AutoValue
public abstract class KafkaRecordToFeatureRowDoFn extends DoFn<KafkaRecord<byte[], byte[]>, FeatureRow> {

  public abstract String getFeatureSetName();

  public abstract int getFeatureSetVersion();

  public abstract Map<String, Field> getFieldByName();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static KafkaRecordToFeatureRowDoFn.Builder newBuilder() {
    return new AutoValue_KafkaRecordToFeatureRowDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSetName(String featureSetName);

    public abstract Builder setFeatureSetVersion(int featureSetVersion);

    public abstract Builder setFieldByName(Map<String, Field> fieldByName);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    public abstract KafkaRecordToFeatureRowDoFn build();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    byte[] value = context.element().getKV().getValue();
    FeatureRow featureRow;

    try {
      featureRow = FeatureRow.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      context.output(
          getFailureTag(),
          FailedElement.newBuilder()
              .setTransformName("KafkaRecordToFeatureRow")
              .setStackTrace(ExceptionUtils.getStackTrace(e))
              .setJobName(context.getPipelineOptions().getJobName())
              .setPayload(new String(Base64.getEncoder().encode(value)))
              .setErrorMessage(e.getMessage())
              .build());
      return;
    }

    // If FeatureRow contains field names that do not exist as EntitySpec
    // or FeatureSpec in FeatureSetSpec, mark the FeatureRow as FailedElement.
    String error = null;
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

    if (error != null) {
      context.output(
          getFailureTag(),
          FailedElement.newBuilder()
              .setTransformName("KafkaRecordToFeatureRow")
              .setJobName(context.getPipelineOptions().getJobName())
              .setPayload(featureRow.toString())
              .setErrorMessage(error)
              .build());
    } else {
      context.output(featureRow);
    }
  }
}
