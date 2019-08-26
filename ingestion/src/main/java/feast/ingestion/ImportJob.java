package feast.ingestion;

import static feast.specs.ImportJobSpecsProto.SourceSpec.SourceType.KAFKA;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.transform.ReadFeaturesTransform;
import feast.ingestion.transform.ToFeatureRowExtended;
import feast.ingestion.transform.WriteFeaturesTransform;
import feast.ingestion.util.ProtoUtil;
import feast.ingestion.util.StorageUtil;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import feast.specs.StorageSpecProto.StorageSpec;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

@SuppressWarnings("WeakerAccess")
@Slf4j
public class ImportJob {

  /**
   * Create and run a Beam pipeline from command line arguments.
   *
   * <p>The arguments will be passed to Beam {@code PipelineOptionsFactory} to create {@code
   * ImportJobPipelineOptions}.
   *
   * <p>The returned PipelineResult object can be used to check the state of the pipeline e.g. if
   * it is running, done or cancelled.
   *
   * @param args command line arguments, typically come from the main() method
   * @return PipelineResult
   * @throws IOException if importJobSpecsUri specified in args is not accessible
   */
  public static PipelineResult runPipeline(String[] args) throws IOException, URISyntaxException {
    ImportJobPipelineOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ImportJobPipelineOptions.class);
    return runPipeline(pipelineOptions);
  }

  /**
   * Create and run a Beam pipeline from {@code ImportJobPipelineOptions}.
   *
   * <p>The returned PipelineResult object can be used to check the state of the pipeline e.g. if
   * it is running, done or cancelled.
   *
   * @param pipelineOptions configuration for the pipeline
   * @return PipelineResult
   * @throws IOException if importJobSpecsUri is not accessible
   */
  public static PipelineResult runPipeline(ImportJobPipelineOptions pipelineOptions)
      throws IOException, URISyntaxException {
    pipelineOptions =
        PipelineOptionsValidator.validate(ImportJobPipelineOptions.class, pipelineOptions);
    ImportJobSpecs importJobSpecs =
        ProtoUtil.createProtoMessageFromYamlFileUri(
            pipelineOptions.getImportJobSpecUri(),
            ImportJobSpecs.newBuilder(),
            ImportJobSpecs.class);
    pipelineOptions.setJobName(importJobSpecs.getJobId());
    setupStorage(importJobSpecs);
    setupConsumerGroupOffset(importJobSpecs);
    Pipeline pipeline = Pipeline.create(pipelineOptions);
    pipeline
        .apply("Read FeatureRow", new ReadFeaturesTransform(importJobSpecs))
        .apply("Create FeatureRowExtended from FeatureRow", new ToFeatureRowExtended())
        .apply("Write FeatureRowExtended", new WriteFeaturesTransform(importJobSpecs));
    return pipeline.run();
  }

  /**
   * Configures the storage for Feast.
   *
   * <p>This method ensures that the storage backend is running and accessible by the import job,
   * and it also ensures that the storage backend has the necessary schema and configuration so the
   * import job can start writing Feature Row.
   *
   * <p>For example, when using BigQuery as the storage backend, this method ensures that, given a
   * list of features, the corresponding BigQuery dataset and table are created.
   *
   * @param importJobSpecs import job specification, refer to {@code ImportJobSpecs.proto}
   */
  private static void setupStorage(ImportJobSpecs importJobSpecs) {
    StorageSpec sinkStorageSpec = importJobSpecs.getSinkStorageSpec();
    String storageSpecType = sinkStorageSpec.getType();

    switch (storageSpecType) {
      case "BIGQUERY":
        StorageUtil.setupBigQuery(
            importJobSpecs.getSinkStorageSpec(),
            importJobSpecs.getEntitySpec(),
            importJobSpecs.getFeatureSpecsList(),
            BigQueryOptions.getDefaultInstance().getService());
        break;
      case "REDIS":
        StorageUtil.checkRedisConnection(sinkStorageSpec);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type of sinkStorageSpec: \"%s\". Only REDIS and BIGQUERY are supported in Feast 0.2",
                storageSpecType));
    }
  }

  /**
   * Manually sets the consumer group offset for this job's consumer group to the offset at the time
   * at which we provision the ingestion job.
   *
   * <p>This is necessary because the setup time for certain
   * runners (e.g. Dataflow) might cause the worker to miss the messages that were emitted into the
   * stream prior to the workers being ready.
   *
   * @param importJobSpecs import job specification, refer to {@code ImportJobSpecs.proto}
   */
  private static void setupConsumerGroupOffset(ImportJobSpecs importJobSpecs) {
    SourceType sourceType = importJobSpecs.getSourceSpec().getType();
    switch (sourceType) {
      case KAFKA:
        String consumerGroupId = String.format("feast-import-job-%s", importJobSpecs.getJobId());
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("group.id", consumerGroupId);
        consumerProperties.setProperty("bootstrap.servers",
            importJobSpecs.getSourceSpec().getOptionsOrThrow("bootstrapServers"));
        consumerProperties.setProperty(
            "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.setProperty(
            "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(consumerProperties);

        String[] topics = importJobSpecs.getSourceSpec().getOptionsOrThrow("topics").split(",");
        long timestamp = System.currentTimeMillis();
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (String topic : topics) {
          List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
          for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
            timestampsToSearch.put(topicPartition, timestamp);
          }
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer
            .offsetsForTimes(timestampsToSearch);

        kafkaConsumer.assign(offsets.keySet());
        kafkaConsumer.poll(1000);
        kafkaConsumer.commitSync();

        offsets.forEach((topicPartition, offset) -> {
          if (offset != null) {
            kafkaConsumer.seek(topicPartition, offset.offset());
          } else {
            kafkaConsumer.seekToBeginning(Sets.newHashSet(topicPartition));
          }
        });
        return;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type of sourceSpec: \"%s\". Only KAFKA is supported in Feast 0.2",
                sourceType));
    }

  }
}
