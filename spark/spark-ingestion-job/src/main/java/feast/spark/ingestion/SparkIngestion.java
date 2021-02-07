/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.spark.ingestion;

import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.IngestionJobProto.FeatureSetSpecAck;
import feast.proto.core.IngestionJobProto.SpecsStreamingUpdateConfig;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto.Store;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.spark.ingestion.common.AllocatedRowWithValidationResult;
import feast.spark.ingestion.common.FailedElement;
import feast.spark.ingestion.common.SpecUtil;
import feast.spark.ingestion.delta.SparkDeltaSink;
import feast.spark.ingestion.metrics.writers.FeastMetricsWriter;
import feast.spark.ingestion.metrics.writers.MetricsWriter;
import feast.spark.ingestion.metrics.writers.NoopMetricsWriter;
import feast.spark.ingestion.redis.SparkRedisSink;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes messages from one or more topics in Kafka and outputs them to Delta/Redis using Spark.
 */
@SuppressWarnings("serial")
public class SparkIngestion implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(SparkIngestion.class);
  private final SparkSession spark;
  private final String jobId;
  private final String checkpointLocation;
  private final String deadLetterPath;
  private final Map<FeatureSetReference, FeatureSetSpec> featureSetSpecs =
      Collections.synchronizedMap(new HashMap<>());
  private final List<Store> stores;
  private final Source source;
  private final SpecsStreamingUpdateConfig specsStreamingUpdateConfig;
  private final FeatureRowToStoreAllocator featureRowToStoreAllocator =
      new FeatureRowToStoreAllocator();
  private final FeatureRowValidator featureRowValidator;
  private StreamingQuery query;

  private MetricsWriter<FeatureRow> metrics;
  private DeadLetterHandler deadLetterHandler;
  private final String metricsExporterType;
  volatile boolean stop = false;

  /**
   * Run a Spark ingestion job.
   *
   * @param args List of parameters:
   *     <p>job-name Job name.
   *     <p>default-project Default feast project to apply to incoming rows that do not specify
   *     project in its feature set reference.
   *     <p>feature-sets-json Feature set definitions (in JSON Lines text format).
   *     <p>stores-json Store set definitions (in JSON Lines text format).
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    SparkIngestion ingestion = new SparkIngestion(args);
    ingestion.run();
  }

  /**
   * Build a Spark ingestion job.
   *
   * @param args List of Spark parameters.
   * @throws InvalidProtocolBufferException
   */
  public SparkIngestion(String[] args) throws InvalidProtocolBufferException {
    int numArgs = 10;
    if (args.length != numArgs) {
      throw new IllegalArgumentException("Expecting " + numArgs + " arguments");
    }

    int index = 0;
    jobId = args[index++];
    String specsStreamingUpdateConfigJson = args[index++];
    checkpointLocation = args[index++];
    featureRowValidator = new FeatureRowValidator(args[index++]);
    deadLetterPath = args[index++];
    String storesJson = args[index++];
    String sourceJson = args[index++];
    metricsExporterType = StringUtils.trimToNull(args[index++]);

    stores = SpecUtil.parseStoreJsonList(Arrays.asList(storesJson.split("\n")));
    source = SpecUtil.parseSourceJson(sourceJson);
    specsStreamingUpdateConfig =
        SpecUtil.parseSpecsStreamingUpdateConfig(specsStreamingUpdateConfigJson);
    log.info("Stores: {}", stores);

    Builder builder = SparkSession.builder().appName("SparkIngestion");

    spark = builder.getOrCreate();
  }

  public void run() throws TimeoutException {
    KafkaSourceConfig sourceConfig = specsStreamingUpdateConfig.getSource();
    KafkaSourceConfig ackConfig = specsStreamingUpdateConfig.getAck();

    Properties config1 = new Properties();
    config1.put("group.id", generateConsumerGroupId(jobId));
    config1.put("enable.auto.commit", "false");
    config1.put("bootstrap.servers", sourceConfig.getBootstrapServers());
    config1.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    config1.put(
        "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    Properties config2 = new Properties();
    config2.put("bootstrap.servers", ackConfig.getBootstrapServers());
    config2.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config2.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    log.info("Connecting to Kafka");
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config1);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(config2); ) {

      String topic = sourceConfig.getTopic();
      List<TopicPartition> topicPartitions =
          consumer.partitionsFor(topic).stream()
              .map(i -> new TopicPartition(i.topic(), i.partition()))
              .collect(Collectors.toList());
      consumer.assign(topicPartitions);
      consumer.seekToBeginning(topicPartitions);
      log.info("Consuming");
      while (!stop) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
        if (records.count() == 0) {
          continue;
        }
        log.info("Got {} control records", records.count());
        List<ProducerRecord<String, byte[]>> acks =
            Streams.stream(records)
                .map(
                    record -> {
                      try {
                        String ref = new String(record.key());
                        FeatureSetSpec spec = FeatureSetSpec.parseFrom(record.value());
                        if (filterRelevant(spec)) {
                          featureSetSpecs.compute(
                              FeatureSetReference.parse(ref),
                              (k, v) ->
                                  (v == null)
                                      ? spec
                                      : spec.getVersion() > v.getVersion() ? spec : v);
                        }
                        FeatureSetSpecAck ack =
                            IngestionJobProto.FeatureSetSpecAck.newBuilder()
                                .setFeatureSetReference(ref)
                                .setJobName(jobId)
                                .setFeatureSetVersion(spec.getVersion())
                                .build();
                        return new ProducerRecord<String, byte[]>(
                            ackConfig.getTopic(), ref, ack.toByteArray());
                      } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                      }
                    })
                .collect(Collectors.toList());

        if (query != null) {
          log.info("Stopping query");
          query.stop();
        }

        log.info("Preparing metric writers");
        List<String> storeNames = stores.stream().map(Store::getName).collect(Collectors.toList());
        metrics =
            metricsExporterType != null
                ? new FeastMetricsWriter(jobId, storeNames, Clock.systemUTC())
                : new NoopMetricsWriter<>();

        log.info("Preparing deadletter handler");
        deadLetterHandler = new DeadLetterHandler(deadLetterPath, jobId, featureSetSpecs);

        log.info("Recreating query with {} FeatureSets", featureSetSpecs.size());
        startSparkQuery();
        log.info("Query started");

        acks.stream().forEach(producer::send);
        log.info("Acknowledged {} control records", acks.size());
      }
    }
  }

  /** Creates a Spark Structured Streaming query. */
  public void startSparkQuery() throws TimeoutException {
    KafkaSourceConfig kafkaConfig = source.getKafkaSourceConfig();

    // Create DataSet representing the stream of input lines from kafka
    Dataset<Row> input =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaConfig.getBootstrapServers())
            .option("startingOffsets", "earliest")
            .option("subscribe", kafkaConfig.getTopic())
            // Don't fail the job if the current Kafka offset has been expired
            .option("failOnDataLoss", "false")
            .load();

    List<ImmutablePair<Store, VoidFunction2<Dataset<FeatureRow>, Long>>> consumerSinks =
        stores.stream()
            .map(
                store -> {
                  switch (store.getType()) {
                    case DELTA:
                      return new ImmutablePair<>(
                          store,
                          new SparkDeltaSink(jobId, store.getDeltaConfig(), featureSetSpecs)
                              .configure());
                    case REDIS:
                      return new ImmutablePair<>(
                          store,
                          new SparkRedisSink(store.getRedisConfig(), featureSetSpecs).configure());
                    default:
                      throw new UnsupportedOperationException(
                          "Store " + store + " is not implemented in Spark ingestion");
                  }
                })
            .collect(Collectors.toList());

    // Start running the query that writes the data to sink
    query =
        input
            .map(
                (MapFunction<Row, AllocatedRowWithValidationResult>)
                    row -> {
                      FeatureRow featureRow =
                          featureRowValidator.setDefaults(
                              FeatureRow.parseFrom((byte[]) row.getAs("value")));
                      FeatureSetReference featureSetRef =
                          FeatureSetReference.parse(featureRow.getFeatureSet());
                      FeatureSetSpec featureSet = featureSetSpecs.get(featureSetRef);

                      FailedElement failedElement;
                      if (featureSet == null) {
                        String msg =
                            String.format(
                                "FeatureRow contains invalid featureSetReference %s."
                                    + " Please check that the feature rows are being published"
                                    + " to the correct topic on the feature stream.",
                                featureRow.getFeatureSet());
                        failedElement =
                            FailedElement.newBuilder()
                                .setProjectName(featureSetRef.getProjectName())
                                .setFeatureSetName(featureSetRef.getFeatureSetName())
                                .setTransformName("ValidateFeatureRow")
                                .setJobName(jobId)
                                .setPayload(featureRow.toString())
                                .setErrorMessage(msg)
                                .build();
                      } else {
                        failedElement =
                            featureRowValidator.validateFeatureRow(jobId, featureRow, featureSet);
                      }

                      List<String> subscribedStoreNames =
                          stores.stream()
                              .filter(
                                  store ->
                                      featureRowToStoreAllocator.allocateRowToStore(
                                          featureRow, store))
                              .map(Store::getName)
                              .collect(Collectors.toList());

                      if (failedElement != null) {

                        return AllocatedRowWithValidationResult.newBuilder()
                            .setFeatureRow(featureRow)
                            .setValid(false)
                            .setSubscribedStoreNames(subscribedStoreNames)
                            .setFailedElement(failedElement)
                            .build();
                      }
                      return AllocatedRowWithValidationResult.newBuilder()
                          .setFeatureRow(featureRow)
                          .setValid(true)
                          .setSubscribedStoreNames(subscribedStoreNames)
                          .build();
                    },
                Encoders.kryo(AllocatedRowWithValidationResult.class))
            .writeStream()
            .option(
                "checkpointLocation",
                String.format("%s/%s", checkpointLocation, generateConsumerGroupId(jobId)))
            .foreachBatch(
                (batchDS, batchId) -> {
                  consumerSinks.forEach(
                      pair -> {
                        try {
                          Dataset<FeatureRow> validRows =
                              batchDS
                                  .filter(AllocatedRowWithValidationResult::isValid)
                                  .filter(
                                      (FilterFunction<AllocatedRowWithValidationResult>)
                                          row ->
                                              row.getSubscribedStoreNames()
                                                  .contains(pair.getLeft().getName()))
                                  .map(
                                      (MapFunction<AllocatedRowWithValidationResult, FeatureRow>)
                                          AllocatedRowWithValidationResult::getFeatureRow,
                                      Encoders.kryo(FeatureRow.class));

                          validRows.persist();

                          log.info("Writing metrics");
                          metrics.writeMetrics(pair.left.getName(), validRows, featureSetSpecs);

                          pair.getRight().call(validRows, batchId);

                          validRows.unpersist();
                        } catch (Exception e) {
                          log.error("Error invoking sink", e);
                          throw new RuntimeException(e);
                        }
                      });

                  Dataset<FailedElement> invalidRows =
                      batchDS
                          .filter(
                              (FilterFunction<AllocatedRowWithValidationResult>)
                                  row -> !row.isValid())
                          .map(
                              (MapFunction<AllocatedRowWithValidationResult, FailedElement>)
                                  AllocatedRowWithValidationResult::getFailedElement,
                              Encoders.kryo(FailedElement.class));

                  deadLetterHandler.storeDeadLetter(invalidRows);
                })
            .start();
  }

  private String generateConsumerGroupId(String jobName) {
    String[] split = jobName.split("-");
    String jobNameWithoutTimestamp =
        Arrays.stream(split).limit(split.length - 1).collect(Collectors.joining("-"));
    return "feast_import_job_" + jobNameWithoutTimestamp;
  }

  public Boolean filterRelevant(FeatureSetProto.FeatureSetSpec spec) {
    return stores.stream()
            .anyMatch(
                s ->
                    feast.common.models.Store.isSubscribedToFeatureSet(
                        s.getSubscriptionsList(), spec.getProject(), spec.getName()))
        && spec.getSource().equals(source);
  }
}
