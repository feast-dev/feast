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
import feast.ingestion.transform.ReadFromSource;
import feast.ingestion.transform.fn.ProcessFeatureRowDoFn;
import feast.ingestion.transform.fn.ValidateFeatureRowDoFn;
import feast.ingestion.transform.specs.FilterRelevantFunction;
import feast.ingestion.utils.SpecUtil;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.IngestionJobProto.FeatureSetSpecAck;
import feast.proto.core.IngestionJobProto.SpecsStreamingUpdateConfig;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.StoreProto.Store;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.spark.ingestion.delta.SparkDeltaSink;
import feast.spark.ingestion.redis.SparkRedisSink;
import feast.storage.api.writer.FailedElement;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
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
  private final String defaultFeastProject;
  private final String deadLetterPath;
  private final Map<String, FeatureSetSpec> featureSetSpecs =
      Collections.synchronizedMap(new HashMap<>());
  private final List<Store> stores;
  private final Source source;
  private final SpecsStreamingUpdateConfig specsStreamingUpdateConfig;
  private StreamingQuery query;
  private static final StructType deadLetterType;
  volatile boolean stop = false;

  static {
    StructType schema = new StructType();
    schema = schema.add("timestamp", DataTypes.TimestampType, false);
    schema = schema.add("job_name", DataTypes.StringType, true);
    schema = schema.add("transform_name", DataTypes.StringType, true);
    schema = schema.add("payload", DataTypes.StringType, true);
    schema = schema.add("error_message", DataTypes.StringType, true);
    schema = schema.add("stack_trace", DataTypes.StringType, true);
    deadLetterType = schema;
  }

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
    int numArgs = 7;
    if (args.length != numArgs) {
      throw new IllegalArgumentException("Expecting " + numArgs + " arguments");
    }

    int index = 0;
    jobId = args[index++];
    String specsStreamingUpdateConfigJson = args[index++];
    checkpointLocation = args[index++];
    defaultFeastProject = args[index++];
    deadLetterPath = args[index++];
    String storesJson = args[index++];
    String sourceJson = args[index++];

    stores = SpecUtil.parseStoreJsonList(Arrays.asList(storesJson.split("\n")));
    source = SpecUtil.parseSourceJson(sourceJson);
    specsStreamingUpdateConfig =
        SpecUtil.parseSpecsStreamingUpdateConfig(specsStreamingUpdateConfigJson);
    log.info("Stores: {}", stores);

    // Create session with getOrCreate and do not call SparkContext.stop() nor System.exit() at the
    // end.
    // See https://docs.databricks.com/jobs.html#jar-job-tips
    spark = SparkSession.builder().appName("SparkIngestion").getOrCreate();
  }

  public void run() {
    KafkaSourceConfig sourceConfig = specsStreamingUpdateConfig.getSource();
    KafkaSourceConfig ackConfig = specsStreamingUpdateConfig.getAck();

    FilterRelevantFunction filterRelevantFunction = new FilterRelevantFunction(source, stores);

    Properties config1 = new Properties();
    config1.put("group.id", ReadFromSource.generateConsumerGroupId(jobId));
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
                        if (filterRelevantFunction.apply(KV.of(ref, spec))) {
                          featureSetSpecs.compute(
                              ref,
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
        log.info("Recreating query with {} FeatureSets", featureSetSpecs.size());
        startSparkQuery();
        log.info("Query started");

        acks.stream().forEach(producer::send);
        log.info("Acknowledged {} control records", acks.size());
      }
    }
  }

  /** Creates a Spark Structured Streaming query. */
  public void startSparkQuery() {
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

    Collection<SparkSink> consumers =
        stores.stream()
            .map(
                store -> {
                  switch (store.getType()) {
                    case DELTA:
                      return new SparkDeltaSink(
                          jobId, store.getDeltaConfig(), spark, featureSetSpecs);
                    case REDIS:
                      return new SparkRedisSink(store.getRedisConfig(), featureSetSpecs);
                    default:
                      throw new UnsupportedOperationException(
                          "Store " + store + " is not implemented in Spark ingestor");
                  }
                })
            .collect(Collectors.toList());

    List<VoidFunction2<Dataset<byte[]>, Long>> consumerSinks =
        consumers.stream()
            .map(
                c -> {
                  try {
                    return c.configure();
                  } catch (Exception e) {
                    log.error("Error configuring sink", e);
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    ProcessFeatureRowDoFn processFeature = new ProcessFeatureRowDoFn(defaultFeastProject);

    // Start running the query that writes the data to sink
    query =
        input
            .map(
                r -> {
                  FeatureRow featureRow = FeatureRow.parseFrom((byte[]) r.getAs("value"));
                  FeatureRow el = processFeature.processRow(featureRow);
                  List<FieldProto.Field> fields = new ArrayList<>();
                  FeatureSetSpec featureSet = featureSetSpecs.get(el.getFeatureSet());

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
                            .setTransformName("ValidateFeatureRow")
                            .setJobName(jobId)
                            .setPayload(featureRow.toString())
                            .setErrorMessage(msg)
                            .build();
                  } else {
                    failedElement =
                        ValidateFeatureRowDoFn.validateFeatureRow(el, featureSet, jobId, fields);
                  }

                  if (failedElement != null) {

                    return RowWithValidationResult.newBuilder()
                        .setFeatureRow(el.toByteArray())
                        .setValid(false)
                        .setFailedElement(failedElement)
                        .build();
                  }
                  return RowWithValidationResult.newBuilder()
                      .setFeatureRow(el.toByteArray())
                      .setValid(true)
                      .build();
                },
                Encoders.kryo(RowWithValidationResult.class))
            .writeStream()
            .option(
                "checkpointLocation",
                String.format(
                    "%s/%s", checkpointLocation, ReadFromSource.generateConsumerGroupId(jobId)))
            .foreachBatch(
                (batchDF, batchId) -> {
                  batchDF.persist();
                  Dataset<byte[]> validRows =
                      batchDF
                          .filter(RowWithValidationResult::isValid)
                          .map(RowWithValidationResult::getFeatureRow, Encoders.BINARY());

                  validRows.persist();
                  consumerSinks.forEach(
                      c -> {
                        try {
                          c.call(validRows, batchId);
                        } catch (Exception e) {
                          log.error("Error invoking sink", e);
                          throw new RuntimeException(e);
                        }
                      });
                  validRows.unpersist();

                  storeDeadLetter(batchDF);

                  batchDF.unpersist();
                })
            .start();
  }

  private void storeDeadLetter(Dataset<RowWithValidationResult> batchDF) {
    Dataset<RowWithValidationResult> invalidRows = batchDF.filter(row -> !row.isValid());

    invalidRows
        .map(
            e -> {
              FailedElement element = e.getFailedElement();
              return RowFactory.create(
                  new java.sql.Timestamp(element.getTimestamp().getMillis()),
                  element.getJobName(),
                  element.getTransformName(),
                  element.getPayload(),
                  element.getErrorMessage(),
                  element.getStackTrace());
            },
            RowEncoder.apply(deadLetterType))
        .write()
        .format("delta")
        .mode("append")
        .save(deadLetterPath);
  }
}
