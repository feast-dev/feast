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

import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.IngestionJobProto.SpecsStreamingUpdateConfig;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.DeltaConfig;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FeatureRowProto.FeatureRow.Builder;
import feast.proto.types.FieldProto;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.BoolList;
import feast.proto.types.ValueProto.BytesList;
import feast.proto.types.ValueProto.DoubleList;
import feast.proto.types.ValueProto.FloatList;
import feast.proto.types.ValueProto.Int32List;
import feast.proto.types.ValueProto.Int64List;
import feast.proto.types.ValueProto.StringList;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.spark.ingestion.delta.FeatureRowToSparkRow;
import feast.spark.ingestion.delta.SparkDeltaSink;
import feast.storage.connectors.redis.retriever.FeatureRowDecoder;
import feast.test.TestUtil;
import feast.test.TestUtil.DummyStatsDServer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@Testcontainers
public class SparkIngestionTest {

  private static final String TEST_JOB_ID = "testjob";

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionTest.class.getName());

  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 9092;
  private static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOST + ":" + KAFKA_PORT;
  private static final String KAFKA_TOPIC = "topic_" + System.currentTimeMillis();
  private static final String KAFKA_SPECS_TOPIC = "fstopic_" + System.currentTimeMillis();
  private static final String KAFKA_ACK_TOPIC = "acktopic_" + System.currentTimeMillis();
  private static final long KAFKA_PUBLISH_TIMEOUT_SEC = 10;

  private static final String REDIS_HOST = "localhost";
  private static final int REDIS_PORT = 6379;

  // No of samples of feature row that will be generated and used for testing.
  // Note that larger no of samples will increase completion time for ingestion.
  private static final int IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE = 128;

  @TempDir public Path deltaFolder;

  @TempDir public Path checkpointFolder;

  @TempDir public Path deadLetterFolder;

  public static final String STATSD_SERVER_HOST = "localhost";
  public static final int STATSD_SERVER_PORT = 17255;

  private SparkSession sparkSession;
  private DummyStatsDServer statsDServer;
  private RedisClient redisClient;
  private RedisConfig redisConfig;
  private DeltaConfig deltaConfig;

  @BeforeEach
  public void setupSpark() {
    sparkSession =
        SparkSession.builder()
            .appName(getClass().getName())
            .master("local")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config(
                "spark.metrics.conf.*.sink.statsd.class",
                "org.apache.spark.metrics.sink.StatsdSink")
            .config("spark.metrics.conf.*.sink.statsd.period", "1")
            .config("spark.metrics.executorMetricsSource.enabled", "false")
            .config("spark.metrics.staticSources.enabled", "false")
            .config("spark.metrics.namespace", "feast_ingestion")
            .config("spark.metrics.conf.*.sink.statsd.host", STATSD_SERVER_HOST)
            .config("spark.metrics.conf.*.sink.statsd.port", STATSD_SERVER_PORT)
            .config("spark.kryo.registrator", "feast.spark.ingestion.kryo.ProtobufRegistrator")
            .getOrCreate();
  }

  @BeforeEach
  public void setupStatsD() {
    statsDServer = new DummyStatsDServer(STATSD_SERVER_PORT);
  }

  @BeforeEach
  public void setupRedis() {
    this.redisConfig = RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();

    RedisURI redisuri =
        new RedisURI(
            redisConfig.getHost(), redisConfig.getPort(), java.time.Duration.ofMillis(2000));

    String password = redisConfig.getPass();
    if (StringUtils.trimToNull(password) != null) {
      redisuri.setPassword(password);
    }

    this.redisClient = RedisClient.create(redisuri);
  }

  @BeforeEach
  public void setupDelta() {
    deltaConfig = DeltaConfig.newBuilder().setPath(deltaFolder.toAbsolutePath().toString()).build();
  }

  @AfterEach
  public void teardownSpark() {
    if (sparkSession != null) {
      this.sparkSession.close();
    }
  }

  @AfterEach
  public void teardownStatsD() {
    if (statsDServer != null) {
      statsDServer.stop();
    }
  }

  @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
          new File("src/test/resources/docker-compose/docker-compose-it.yml"));

  @Test
  public void streamingQueryShouldWriteKafkaPayloadAsDeltaLakeAndRedis() throws Exception {
    KafkaSourceConfig kafka =
        KafkaSourceConfig.newBuilder()
            .setBootstrapServers(KAFKA_HOST + ":" + KAFKA_PORT)
            .setTopic(KAFKA_TOPIC)
            .build();

    FeatureSet featureSetForRedis = createFeatureSetForRedis(kafka);
    FeatureSet featureSetForDelta = createFeatureSetForDelta(kafka);
    FeatureSetSpec specForRedis = featureSetForRedis.getSpec();
    FeatureSetSpec specForDelta = featureSetForDelta.getSpec();
    FeatureSetSpec invalidSpec =
        FeatureSetSpec.newBuilder(specForDelta).setProject("invalid_project").build();

    List<FeatureRow> inputForRedis =
        generateTestData(specForRedis, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);

    List<FeatureRow> expectedRedis = new ArrayList<>(inputForRedis);

    // generate duplicate row with older timestamp to check only most recent is kept.
    FeatureRow duplicateRedisRow = generateDuplicateData(inputForRedis);

    List<FeatureRow> inputForDelta =
        generateTestData(specForDelta, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);

    // add duplicate row with different timestamp to check both are ingested.
    inputForDelta.add(generateDuplicateData(inputForDelta));

    List<FeatureRow> invalidInput =
        generateTestData(invalidSpec, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);
    List<Pair<String, FeatureRow>> allInputs =
        Stream.concat(
                Stream.concat(
                    Stream.concat(inputForRedis.stream(), Stream.of(duplicateRedisRow)),
                    inputForDelta.stream()),
                invalidInput.stream())
            .map(f -> Pair.of("dummy", f))
            .collect(Collectors.toList());

    LOGGER.info("Starting Import Job");

    Store redis = createStore(specForRedis, StoreType.REDIS).setRedisConfig(redisConfig).build();
    Store delta = createStore(specForDelta, StoreType.DELTA).setDeltaConfig(deltaConfig).build();

    SpecsStreamingUpdateConfig specsStreamingUpdateConfig =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
            .setSource(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(KAFKA_HOST + ":" + KAFKA_PORT)
                    .setTopic(KAFKA_SPECS_TOPIC)
                    .build())
            .setAck(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(KAFKA_HOST + ":" + KAFKA_PORT)
                    .setTopic(KAFKA_ACK_TOPIC)
                    .build())
            .build();
    String specsStreamingUpdateConfigJson =
        toJsonLines(Collections.singleton(specsStreamingUpdateConfig));

    SourceProto.Source source =
        SourceProto.Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(kafka)
            .build();

    String storesJson = toJsonLines(Arrays.asList(redis, delta));
    String sourceJson = toJsonLines(Collections.singleton(source));
    List<FeatureSet> featureSets = Arrays.asList(featureSetForRedis, featureSetForDelta);
    List<FeatureSetSpec> featureSetSpecs =
        featureSets.stream().map(FeatureSet::getSpec).collect(Collectors.toList());

    Dataset<Row> data = null;

    String checkpointDir = checkpointFolder.toAbsolutePath().toString();
    String deadLetterDir = deadLetterFolder.toAbsolutePath().toString();

    SparkIngestion ingestion =
        new SparkIngestion(
            new String[] {
              TEST_JOB_ID,
              specsStreamingUpdateConfigJson,
              checkpointDir,
              "myDefaultFeastProject",
              deadLetterDir,
              storesJson,
              sourceJson,
              "statsd",
              STATSD_SERVER_HOST,
              String.valueOf(STATSD_SERVER_PORT)
            });

    Thread startup;
    startup =
        new Thread(
            () -> {
              try {
                ingestion.run();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    try {
      startup.start();

      LOGGER.info("Publishing Feature Sets to Kafka");
      TestUtil.publishToKafka(
          KAFKA_BOOTSTRAP_SERVERS,
          KAFKA_SPECS_TOPIC,
          featureSetSpecs.stream()
              .map(f -> Pair.of(getFeatureSetStringRef(f), f))
              .collect(Collectors.toList()),
          ByteArraySerializer.class,
          KAFKA_PUBLISH_TIMEOUT_SEC);

      LOGGER.info("Publishing {} Feature Row messages to Kafka ...", allInputs.size());
      TestUtil.publishToKafka(
          KAFKA_BOOTSTRAP_SERVERS,
          KAFKA_TOPIC,
          allInputs,
          ByteArraySerializer.class,
          KAFKA_PUBLISH_TIMEOUT_SEC);

      String deltaTablePath =
          SparkDeltaSink.getDeltaTablePath(deltaFolder.toAbsolutePath().toString(), specForDelta);

      try {
        for (int i = 0; i < 120; i++) {
          if (Files.isDirectory().apply(new File(deadLetterDir))) {
            try {
              data = sparkSession.read().format("delta").load(deadLetterDir);
            } catch (Exception e) {
              if (e.getMessage().contains("is not a Delta table")) {
                LOGGER.info("Dead letter not yet populated.");
                Thread.sleep(1000L);
                continue;
              }
              throw e;
            }
            long count = data.count();
            LOGGER.info("Dead letter directory contains {} records.", count);
            if (count >= IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE) {
              break;
            }
          } else {
            LOGGER.info("Dead letter directory not yet created.");
          }
          Thread.sleep(1000);
        }
      } finally {
        Arrays.stream(sparkSession.streams().active())
            .forEach(
                q -> {
                  try {
                    q.stop();
                  } catch (TimeoutException e) {
                    throw new IllegalStateException(e);
                  }
                });
      }

      assertThat("Should have returned data", data, notNullValue());
      assertThat("Should have returned data", data.count(), greaterThan(0L));

      validateRedis(featureSetForRedis, featureSetForDelta, inputForRedis, inputForDelta);

      validateDelta(featureSetForDelta, inputForDelta, deltaTablePath);

      validateDeadLetter(invalidInput);
    } finally {
      ingestion.stop = true;
      startup.join();
    }

    // Wait until StatsD has finished processed all messages
    Thread.sleep(10000);

    List<String> expectedStatsDMessages =
        Arrays.asList(
            "feast_ingestion.driver.FeastMetrics.deadletter_row_count#ingestion_job_name=testjob,feast_project_name=myproject,feast_featureSet_name=feature_set_for_delta:0|g",
            "feast_ingestion.driver.FeastMetrics.deadletter_row_count#ingestion_job_name=testjob,feast_project_name=myproject,feast_featureSet_name=feature_set_for_redis:0|g",
            "feast_ingestion.driver.FeastMetrics.feature_row_ingested_count#metrics_namespace=Inflight,ingestion_job_name=testjob,feast_project_name=myproject,feast_featureSet_name=feature_set_for_delta,feast_store=REDIS:0|g",
            "feast_ingestion.driver.FeastMetrics.feature_row_ingested_count#metrics_namespace=Inflight,ingestion_job_name=testjob,feast_project_name=myproject,feast_featureSet_name=feature_set_for_redis,feast_store=REDIS:129|g",
            "feast_ingestion.driver.FeastMetrics.feature_row_ingested_count#metrics_namespace=Inflight,ingestion_job_name=testjob,feast_project_name=myproject,feast_featureSet_name=feature_set_for_redis,feast_store=DELTA:0|g",
            "feast_ingestion.driver.FeastMetrics.feature_row_ingested_count#metrics_namespace=Inflight,ingestion_job_name=testjob,feast_project_name=myproject,feast_featureSet_name=feature_set_for_delta,feast_store=DELTA:129|g",
            "feast_ingestion.driver.FeastMetrics.feature_value_min",
            "feast_ingestion.driver.FeastMetrics.feature_value_max",
            "feast_ingestion.driver.FeastMetrics.feature_value_mean",
            "feast_ingestion.driver.FeastMetrics.feature_row_lag_ms_min",
            "feast_ingestion.driver.FeastMetrics.feature_row_lag_ms_max",
            "feast_ingestion.driver.FeastMetrics.feature_row_lag_ms_mean",
            "feast_ingestion.driver.FeastMetrics.feature_name_missing_count",
            "feast_ingestion.driver.FeastMetrics.feature_value_p25",
            "feast_ingestion.driver.FeastMetrics.feature_value_p50",
            "feast_ingestion.driver.FeastMetrics.feature_value_p90",
            "feast_ingestion.driver.FeastMetrics.feature_value_p95",
            "feast_ingestion.driver.FeastMetrics.feature_value_p99");

    List<String> actualLines = statsDServer.messagesReceived();

    ArrayList<String> distinctActualLines = new ArrayList<>(new HashSet<>(actualLines));

    LOGGER.info("Number of Distinct StatsD Messages: " + distinctActualLines.size());

    statsDMessagesCompare(expectedStatsDMessages, distinctActualLines);
  }

  public static void statsDMessagesCompare(List<String> expectedLines, List<String> actualLines) {
    for (String expected : expectedLines) {
      boolean matched = false;
      for (String actual : actualLines) {

        if (actual.startsWith(expected)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        System.out.println("Print actual metrics output for debugging:");
        for (String line : actualLines) {
          System.out.println(line);
        }
        fail(String.format("Expected StatsD metric not found:\n%s", expected));
      }
    }
  }

  private <T extends MessageOrBuilder> String toJsonLines(Collection<T> items) {
    return items.stream()
        .map(
            new Function<T, String>() {
              @Override
              public String apply(T item) {
                try {
                  return JsonFormat.printer()
                      .omittingInsignificantWhitespace()
                      .printingEnumsAsInts()
                      .print(item);
                } catch (InvalidProtocolBufferException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .collect(Collectors.joining("\n"));
  }

  private void validateDelta(FeatureSet featureSet, List<FeatureRow> input, String deltaTablePath) {
    LOGGER.info("Validating the actual values written to Delta ...");

    Dataset<Row> data = sparkSession.read().format("delta").load(deltaTablePath);

    Set<FeatureRow> delta =
        data.limit(IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE + 1).collectAsList().stream()
            .map(row -> sparkRowToFeatureRow(featureSet.getSpec(), row))
            .collect(Collectors.toSet());

    // Ensure each of the retrieved FeatureRow is equal to the ingested FeatureRow.
    assertThat(delta.size(), is(input.size()));
    assertThat(delta, is(new HashSet<>(input)));
  }

  private void validateDeadLetter(List<FeatureRow> invalidInput) throws Exception {
    String deadLetterDir = deadLetterFolder.toAbsolutePath().toString();
    for (int i = 0; i < 60; i++) {

      Dataset<Row> data = sparkSession.read().format("delta").load(deadLetterDir);
      long count = data.count();
      assertThat(count, is((long) IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE));
      Row f = data.first();
      if (f.length() > 0) {
        break;
      } else {
        LOGGER.info("Delta directory not yet created.");
      }
      Thread.sleep(1000);
    }

    Dataset<Row> data = sparkSession.read().format("delta").load(deadLetterDir);
    long count = data.count();
    assertThat(count, is((long) IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE));
    Row f = data.first();
    assertThat(f.length(), is(6));
    int i = 0;
    assertThat("timestamp", f.get(i++), instanceOf(java.sql.Timestamp.class));
    assertThat("jobName", (String) f.getAs(i++), equalTo(TEST_JOB_ID));
    assertThat("transformName", (String) f.getAs(i++), is("ValidateFeatureRow"));
    assertThat("payload", (String) f.getAs(i++), startsWith("fields"));
    assertThat(
        "errorMessage",
        (String) f.getAs(i++),
        containsString("FeatureRow contains invalid featureSetReference"));
    assertThat("stackTrace", (String) f.getAs(i++), equalTo(null));
  }

  public static FeatureRow sparkRowToFeatureRow(FeatureSetSpec featureSetSpec, Row row) {
    java.sql.Timestamp ts = row.getAs(FeatureRowToSparkRow.EVENT_TIMESTAMP_COLUMN);
    Builder builder =
        FeatureRow.newBuilder()
            .setFeatureSet(getFeatureSetStringRef(featureSetSpec))
            .setEventTimestamp(Timestamps.fromMillis(ts.getTime()));

    featureSetSpec
        .getEntitiesList()
        .forEach(
            field -> {
              builder.addFields(
                  Field.newBuilder()
                      .setName(field.getName())
                      .setValue(
                          sparkValueToFeatureValue(
                              field.getValueType(), row.getAs(field.getName())))
                      .build());
            });

    featureSetSpec
        .getFeaturesList()
        .forEach(
            field -> {
              builder.addFields(
                  Field.newBuilder()
                      .setName(field.getName())
                      .setValue(
                          sparkValueToFeatureValue(
                              field.getValueType(), row.getAs(field.getName())))
                      .build());
            });

    return builder.build();
  }

  public static Value sparkValueToFeatureValue(ValueType.Enum type, Object object) {
    Value.Builder builder = Value.newBuilder();

    if (object == null) {
      return builder.build();
    }

    switch (type) {
      case BYTES:
        builder.setBytesVal(ByteString.copyFrom((byte[]) object));
        break;
      case STRING:
        builder.setStringVal((String) object);
        break;
      case INT32:
        builder.setInt32Val((int) object);
        break;
      case INT64:
        builder.setInt64Val((long) object);
        break;
      case DOUBLE:
        builder.setDoubleVal((double) object);
        break;
      case FLOAT:
        builder.setFloatVal((float) object);
        break;
      case BOOL:
        builder.setBoolVal((boolean) object);
        break;
      case BYTES_LIST:
        builder.setBytesListVal(BytesList.newBuilder().addAllVal(sparkArrayToIterable(object)));
        break;
      case STRING_LIST:
        builder.setStringListVal(StringList.newBuilder().addAllVal(sparkArrayToIterable(object)));
        break;
      case INT32_LIST:
        builder.setInt32ListVal(Int32List.newBuilder().addAllVal(sparkArrayToIterable(object)));
        break;
      case INT64_LIST:
        builder.setInt64ListVal(Int64List.newBuilder().addAllVal(sparkArrayToIterable(object)));
        break;
      case DOUBLE_LIST:
        builder.setDoubleListVal(DoubleList.newBuilder().addAllVal(sparkArrayToIterable(object)));
        break;
      case FLOAT_LIST:
        builder.setFloatListVal(FloatList.newBuilder().addAllVal(sparkArrayToIterable(object)));
        break;
      case BOOL_LIST:
        builder.setBoolListVal(BoolList.newBuilder().addAllVal(sparkArrayToIterable(object)));
        break;
      default:
        throw new IllegalArgumentException("Unsupported ValueType: " + type);
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private static <T> Iterable<T> sparkArrayToIterable(Object object) {
    return JavaConverters.seqAsJavaListConverter((Seq<T>) object).asJava();
  }

  /**
   * Publish test Feature Row messages to a running Kafka broker
   *
   * @param bootstrapServers e.g. localhost:9092
   * @param topic e.g. my_topic
   * @param messages e.g. list of Feature Row
   * @param valueSerializer in Feast this valueSerializer should be "ByteArraySerializer.class"
   * @param publishTimeoutSec duration to wait for publish operation (of each message) to succeed
   */
  public static void publishFeatureRowsToKafka(
      String bootstrapServers,
      String topic,
      List<FeatureRow> messages,
      Class<?> valueSerializer,
      long publishTimeoutSec) {
    Long defaultKey = 1L;
    Properties prop = new Properties();
    prop.put("bootstrap.servers", bootstrapServers);
    prop.put("key.serializer", LongSerializer.class);
    prop.put("value.serializer", valueSerializer);
    try (Producer<Long, byte[]> producer = new KafkaProducer<>(prop)) {

      messages.forEach(
          featureRow -> {
            ProducerRecord<Long, byte[]> record =
                new ProducerRecord<>(topic, defaultKey, featureRow.toByteArray());
            try {
              producer.send(record).get(publishTimeoutSec, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  public static FeatureSet createFeatureSetForRedis(KafkaSourceConfig sourceConfig) {
    FeatureSetSpec spec1 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set_for_redis")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(featureOfType(Enum.BYTES))
            .addFeatures(featureOfType(Enum.STRING))
            .addFeatures(featureOfType(Enum.INT32))
            .addFeatures(featureOfType(Enum.INT64))
            .addFeatures(featureOfType(Enum.DOUBLE))
            .addFeatures(featureOfType(Enum.FLOAT))
            .addFeatures(featureOfType(Enum.BOOL))
            .addFeatures(featureOfType(Enum.BYTES_LIST))
            .addFeatures(featureOfType(Enum.STRING_LIST))
            .addFeatures(featureOfType(Enum.INT32_LIST))
            .addFeatures(featureOfType(Enum.INT64_LIST))
            .addFeatures(featureOfType(Enum.DOUBLE_LIST))
            .addFeatures(featureOfType(Enum.FLOAT_LIST))
            .addFeatures(featureOfType(Enum.BOOL_LIST))
            .setSource(
                Source.newBuilder()
                    .setType(SourceType.KAFKA)
                    .setKafkaSourceConfig(sourceConfig)
                    .build())
            .build();

    return FeatureSet.newBuilder().setSpec(spec1).build();
  }

  public static FeatureSet createFeatureSetForDelta(KafkaSourceConfig sourceConfig) {
    FeatureSetSpec spec1 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set_for_delta")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(featureOfType(Enum.BYTES))
            .addFeatures(featureOfType(Enum.STRING))
            .addFeatures(featureOfType(Enum.INT32))
            .addFeatures(featureOfType(Enum.INT64))
            .addFeatures(featureOfType(Enum.DOUBLE))
            .addFeatures(featureOfType(Enum.FLOAT))
            .addFeatures(featureOfType(Enum.BOOL))
            // FIXME causes roundtrip assertion error with Spark
            // .addFeatures(featureOfType(Enum.BYTES_LIST))
            .addFeatures(featureOfType(Enum.STRING_LIST))
            .addFeatures(featureOfType(Enum.INT32_LIST))
            .addFeatures(featureOfType(Enum.INT64_LIST))
            .addFeatures(featureOfType(Enum.DOUBLE_LIST))
            .addFeatures(featureOfType(Enum.FLOAT_LIST))
            .addFeatures(featureOfType(Enum.BOOL_LIST))
            .setSource(
                Source.newBuilder()
                    .setType(SourceType.KAFKA)
                    .setKafkaSourceConfig(sourceConfig)
                    .build())
            .build();

    FeatureSet featureSet = FeatureSet.newBuilder().setSpec(spec1).build();
    return featureSet;
  }

  private static FeatureSpec featureOfType(Enum type) {
    return FeatureSpec.newBuilder().setName("f_" + type.name()).setValueType(type).build();
  }

  public static List<FeatureRow> generateTestData(FeatureSetSpec spec, int size) {
    LOGGER.info("Generating test data ...");
    List<FeatureRow> input = new ArrayList<>();
    IntStream.range(0, size)
        .forEach(
            i -> {
              FeatureRow randomRow = TestUtil.createRandomFeatureRow(spec);
              input.add(randomRow);
            });
    return input;
  }

  private static FeatureRow generateDuplicateData(List<FeatureRow> input) {
    FeatureRow sampleRow = input.get(0);

    Builder duplicateRowBuilder =
        FeatureRow.newBuilder()
            .setFeatureSet(sampleRow.getFeatureSet())
            .setEventTimestamp(
                Timestamps.subtract(sampleRow.getEventTimestamp(), Durations.fromDays(1)));
    sampleRow.getFieldsList().forEach(duplicateRowBuilder::addFields);
    return duplicateRowBuilder.build();
  }

  public static Map<RedisKey, FeatureRow> generateExpectedData(
      FeatureSetSpec spec, List<FeatureRow> featureRows) {

    HashMap<RedisKey, FeatureRow> expected = new HashMap<>();
    featureRows.stream()
        .forEach(
            randomRow -> {
              RedisKey redisKey = TestUtil.createRedisKey(spec, randomRow);
              List<FieldProto.Field> fields =
                  randomRow.getFieldsList().stream()
                      .filter(
                          field ->
                              spec.getFeaturesList().stream()
                                  .map(FeatureSpec::getName)
                                  .collect(Collectors.toList())
                                  .contains(field.getName()))
                      .sorted(Comparator.comparing(Field::getName))
                      .map(field -> field.toBuilder().build())
                      .collect(Collectors.toList());
              randomRow = randomRow.toBuilder().clearFields().addAllFields(fields).build();
              expected.put(redisKey, randomRow);
            });
    return expected;
  }

  public static Store.Builder createStore(FeatureSetSpec spec, StoreType storeType) {
    return Store.newBuilder()
        .setName(storeType.toString())
        .setType(storeType)
        .addSubscriptions(
            Subscription.newBuilder()
                .setProject(spec.getProject())
                .setName(spec.getName())
                .build());
  }

  public void validateRedis(
      FeatureSet redisFeatureSet,
      FeatureSet deltaFeatureSet,
      List<FeatureRow> expectedRedis,
      List<FeatureRow> unexpectedDelta) {

    FeatureRowDecoder featureRowDecoder =
        new FeatureRowDecoder(
            getFeatureSetStringRef(redisFeatureSet.getSpec()), redisFeatureSet.getSpec());

    Map<RedisKey, FeatureRow> expected =
        generateExpectedData(redisFeatureSet.getSpec(), expectedRedis);

    Map<RedisKey, FeatureRow> unexpectedDeltaRows =
        generateExpectedData(deltaFeatureSet.getSpec(), unexpectedDelta);

    LOGGER.info("Validating the actual values written to Redis ...");

    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    RedisCommands<byte[], byte[]> sync = connection.sync();

    expected.forEach(
        (key, expectedValue) -> {

          // Ensure ingested key exists.
          byte[] actualByteValue = sync.get(key.toByteArray());
          if (actualByteValue == null) {
            LOGGER.error("Key not found in Redis: " + key);
            LOGGER.info("Redis INFO:");
            LOGGER.info(sync.info());
            byte[] randomKey = sync.randomkey();
            if (randomKey != null) {
              LOGGER.info("Sample random key, value (for debugging purpose):");
              LOGGER.info("Key: " + randomKey);
              try {
                LOGGER.info(
                    "Value: "
                        + featureRowDecoder.decode(FeatureRow.parseFrom(sync.get(randomKey))));
              } catch (InvalidProtocolBufferException e) {
                fail(e);
              }
            }
            fail("Missing key in Redis.");
          }

          // Ensure value is a valid serialized FeatureRow object.
          FeatureRow actualValue = null;
          try {
            // Need to decode the feature row
            actualValue = featureRowDecoder.decode(FeatureRow.parseFrom(actualByteValue));
            List<Field> fields = actualValue.getFieldsList();
            actualValue =
                actualValue
                    .toBuilder()
                    .clearFields()
                    .addAllFields(
                        fields.stream()
                            .sorted(Comparator.comparing(Field::getName))
                            .collect(Collectors.toList()))
                    .build();
          } catch (InvalidProtocolBufferException e) {
            fail(
                String.format(
                    "Actual Redis value cannot be parsed as FeatureRow, key: %s, value :%s",
                    key, new String(actualByteValue, StandardCharsets.UTF_8)));
          }

          // Ensure the retrieved FeatureRow is equal to the ingested FeatureRow.
          assertThat(actualValue, is(expectedValue));
        });

    unexpectedDeltaRows.forEach(
        (key, expectedValue) -> {
          byte[] actualByteValue = sync.get(key.toByteArray());
          if (actualByteValue != null) {
            fail(
                String.format(
                    "Actual Redis value was found for rows allocated to delta, key: %s, value :%s",
                    key, new String(actualByteValue, StandardCharsets.UTF_8)));
          }
        });

    redisClient.shutdown();
  }
}
