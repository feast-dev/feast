package feast.ingestion;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.Source.SourceType;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.transform.ReadFeatureRow;
import feast.ingestion.transform.ToFeatureRowExtended;
import feast.ingestion.util.StorageUtil;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class ImportJob {
  /**
   * Create and run a Beam pipeline with PipelineOptions passed as a list of string arguments.
   *
   * <p>The arguments will be passed to Beam {@code PipelineOptionsFactory} to create {@code
   * ImportJobPipelineOptions}.
   *
   * <p>The returned PipelineResult object can be used to check the state of the pipeline e.g. if it
   * is running, done or cancelled.
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
   * <p>The returned PipelineResult object can be used to check the state of the pipeline e.g. if it
   * is running, done or cancelled.
   *
   * @param pipelineOptions configuration for the pipeline
   * @return PipelineResult
   * @throws IOException if importJobSpecsUri is not accessible
   */
  public static PipelineResult runPipeline(ImportJobPipelineOptions pipelineOptions)
      throws IOException {
    pipelineOptions =
        PipelineOptionsValidator.validate(ImportJobPipelineOptions.class, pipelineOptions);
    Pipeline pipeline = Pipeline.create(pipelineOptions);

    for (String storeJson : pipelineOptions.getStoreJson()) {
      Store.Builder storeBuilder = Store.newBuilder();
      JsonFormat.parser().merge(storeJson, storeBuilder);
      Store store = storeBuilder.build();

      for (Subscription subscription : store.getSubscriptionsList()) {
        // TODO: handle version ranges and keyword (e.g. latest) in subscription

        for (String featureSetSpecJson : pipelineOptions.getFeatureSetSpecJson()) {
          FeatureSetSpec.Builder featureSetSpecBuilder = FeatureSetSpec.newBuilder();
          JsonFormat.parser().merge(featureSetSpecJson, featureSetSpecBuilder);
          FeatureSetSpec featureSetSpec = featureSetSpecBuilder.build();

          if (subscription.getName().equalsIgnoreCase(featureSetSpec.getName())
              && subscription
                  .getVersion()
                  .equalsIgnoreCase(String.valueOf(featureSetSpec.getVersion()))) {
            setupSource(featureSetSpec.getSource());
            setupStore(store, featureSetSpec);
          }

          pipeline
              .apply("Read FeatureRow", new ReadFeatureRow(featureSetSpec))
              .apply("Create FeatureRowExtended from FeatureRow", new ToFeatureRowExtended());
          // .apply("Write FeatureRowExtended", new WriteFeaturesTransform(featureSetSpec));
        }
      }
    }

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
   * @param store Store specification, refer to {@code feast.core.Store.proto}
   */
  private static void setupStore(Store store, FeatureSetSpec featureSetSpec) {
    switch (store.getType()) {
      case REDIS:
        StorageUtil.checkRedisConnection(store.getRedisConfig());
        break;
      case BIGQUERY:
        StorageUtil.setupBigQuery(
            featureSetSpec,
            store.getBigqueryConfig().getProjectId(),
            store.getBigqueryConfig().getDatasetId(),
            BigQueryOptions.getDefaultInstance().getService());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Store type: %s not implemented yet", store.getType()));
    }
  }

  /**
   * TODO: Update documentation
   *
   * <p>Manually sets the consumer group offset for this job's consumer group to the offset at the
   * time at which we provision the ingestion job.
   *
   * <p>This is necessary because the setup time for certain runners (e.g. Dataflow) might cause the
   * worker to miss the messages that were emitted into the stream prior to the workers being ready.
   */
  private static void setupSource(Source source) {
    if (!source.getType().equals(SourceType.KAFKA)) {
      throw new UnsupportedOperationException(
          String.format("Source type: %s not implemented yet", source.getType()));
    }

    if (!source.getOptions().containsKey("consumerGroupId")) {
      log.warn(
          "consumerGroupId is not provided in the source options. Import job will not be able to resume correctly from existing checkpoint.");
      return;
    }

    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("group.id", source.getOptionsOrThrow("consumerGroupId"));
    consumerProperties.setProperty(
        "bootstrap.servers", source.getOptionsOrThrow("bootstrapServers"));
    consumerProperties.setProperty(
        "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.setProperty(
        "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer kafkaConsumer = new KafkaConsumer(consumerProperties);

    String[] topics = source.getOptionsOrThrow("topics").split(",");
    long timestamp = System.currentTimeMillis();
    Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
    for (String topic : topics) {
      List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
      for (PartitionInfo partitionInfo : partitionInfos) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
        timestampsToSearch.put(topicPartition, timestamp);
      }
    }
    Map<TopicPartition, OffsetAndTimestamp> offsets =
        kafkaConsumer.offsetsForTimes(timestampsToSearch);

    kafkaConsumer.assign(offsets.keySet());
    kafkaConsumer.poll(1000);
    kafkaConsumer.commitSync();

    offsets.forEach(
        (topicPartition, offset) -> {
          if (offset != null) {
            kafkaConsumer.seek(topicPartition, offset.offset());
          } else {
            kafkaConsumer.seekToBeginning(Sets.newHashSet(topicPartition));
          }
        });
  }
}
