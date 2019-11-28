package feast.ingestion.transform;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.ingestion.transform.fn.KafkaRecordToFeatureRowDoFn;
import feast.ingestion.values.FailedElement;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

@AutoValue
public abstract class ReadFromSource extends PTransform<PBegin, PCollectionTuple> {

  public abstract Source getSource();

  public abstract TupleTag<FeatureRow> getSuccessTag();

  public abstract TupleTag<FailedElement> getFailureTag();

  public static Builder newBuilder() {
    return new AutoValue_ReadFromSource.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSource(Source source);

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
                    KafkaRecordToFeatureRowDoFn.newBuilder()
                        .setSuccessTag(getSuccessTag())
                        .setFailureTag(getFailureTag())
                        .build())
                .withOutputTags(getSuccessTag(), TupleTagList.of(getFailureTag())));
  }

  private String generateConsumerGroupId(String jobName) {
    return "feast_import_job_" + jobName;
  }
}
