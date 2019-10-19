package feast.ingestion.transform;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.ingestion.values.FailedElement;
import feast.ingestion.values.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto;
import feast.types.ValueProto.Value.ValCase;
import java.util.Base64;
import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.exception.ExceptionUtils;

@AutoValue
public abstract class ReadFromSource extends PTransform<PBegin, PCollectionTuple> {
  public abstract Source getSource();

  public abstract Map<String, Field> getFieldByName();

  public abstract String getFeatureSetName();

  public abstract int getFeatureSetVersion();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ReadFromSource.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSource(Source source);

    public abstract Builder setFeatureSetName(String featureSetName);

    public abstract Builder setFeatureSetVersion(int featureSetVersion);

    public abstract Builder setFieldByName(Map<String, Field> fieldByName);

    public abstract Builder setSuccessTag(TupleTag<FeatureRow> successTag);

    public abstract Builder setFailureTag(TupleTag<FailedElement> failureTag);

    abstract ReadFromSource autobuild();

    public ReadFromSource build() {
      ReadFromSource read = autobuild();
      Source source = read.getSource();
      Preconditions.checkState(
          source.getType().equals(SourceType.KAFKA),
          "Source type must be KAFKA. Please raise an issue in https://github.com/gojek/feast/issues to request additional source types.");
      Preconditions.checkState(
          !source.getKafkaSourceConfig().getBootstrapServers().isEmpty(),
          "bootstrap_servers cannot be empty.");
      Preconditions.checkState(
          !source.getKafkaSourceConfig().getTopic().isEmpty(), "topic cannot be empty.");
      return read;
    }
  }

  @Override
  public PCollectionTuple expand(PBegin input) {
    return input
        .getPipeline()
        .apply(
            "ReadFromKafka",
            KafkaIO.readBytes()
                .withBootstrapServers(getSource().getKafkaSourceConfig().getBootstrapServers())
                .withTopic(getSource().getKafkaSourceConfig().getTopic())
                .withConsumerConfigUpdates(
                    ImmutableMap.of(
                        "group.id",
                        generateConsumerGroupId(input.getPipeline().getOptions().getJobName())))
                .withReadCommitted()
                .commitOffsetsInFinalize())
        .apply(
            "KafkaRecordToFeatureRow",
            ParDo.of(
                    new DoFn<KafkaRecord<byte[], byte[]>, FeatureRow>() {
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
                    })
                .withOutputTags(getSuccessTag(), TupleTagList.of(getFailureTag())));
  }

  private String generateConsumerGroupId(String jobName) {
    return "feast_import_job_" + jobName;
  }
}
