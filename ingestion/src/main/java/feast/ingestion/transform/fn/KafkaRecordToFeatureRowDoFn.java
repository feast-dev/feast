package feast.ingestion.transform.fn;

import com.google.auto.value.AutoValue;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.ingestion.transform.ReadFromSource.Builder;
import feast.ingestion.values.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Base64;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.exception.ExceptionUtils;

@AutoValue
public abstract class KafkaRecordToFeatureRowDoFn
    extends DoFn<KafkaRecord<byte[], byte[]>, FeatureRow> {

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static KafkaRecordToFeatureRowDoFn.Builder newBuilder() {
    return new AutoValue_KafkaRecordToFeatureRowDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

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
    context.output(featureRow);
  }
}
