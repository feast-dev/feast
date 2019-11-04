package feast.ingestion.transform.fn;

import com.google.auto.value.AutoValue;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.ingestion.transform.ReadFromSource;
import feast.ingestion.transform.ReadFromSource.Builder;
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
public abstract class KafkaRecordToFeatureRowDoFn extends
    DoFn<KafkaRecord<byte[], byte[]>, FeatureRow> {
  public abstract Map<String, TupleTag<FeatureRow>> getFeatureSetTagByKey();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static KafkaRecordToFeatureRowDoFn.Builder newBuilder() {
    return new AutoValue_KafkaRecordToFeatureRowDoFn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureSetTagByKey(Map<String, TupleTag<FeatureRow>> featureSetTagByKey);

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
    TupleTag<FeatureRow> tag = getFeatureSetTagByKey()
        .getOrDefault(featureRow.getFeatureSet(), null);
    if (tag == null) {
      context.output(
          getFailureTag(),
          FailedElement.newBuilder()
              .setTransformName("KafkaRecordToFeatureRow")
              .setJobName(context.getPipelineOptions().getJobName())
              .setPayload(new String(Base64.getEncoder().encode(value)))
              .setErrorMessage(String.format("Got row with unexpected feature set id %s. Expected one of %s.", featureRow.getFeatureSet(), getFeatureSetTagByKey().keySet()))
              .build());
      return;
    }
    context.output(tag, featureRow);
  }
}
