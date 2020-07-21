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
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
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
import feast.test.TestUtil;
import feast.test.TestUtil.LocalKafka;
import feast.test.TestUtil.LocalRedis;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class SparkIngestionTest {
  private static final String TEST_JOB_ID = "testjob";

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionTest.class.getName());

  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 19092;
  private static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOST + ":" + KAFKA_PORT;
  private static final short KAFKA_REPLICATION_FACTOR = 1;
  private static final String KAFKA_TOPIC = "topic_" + System.currentTimeMillis();
  private static final String KAFKA_SPECS_TOPIC = "fstopic_" + System.currentTimeMillis();
  private static final String KAFKA_ACK_TOPIC = "acktopic_" + System.currentTimeMillis();
  private static final long KAFKA_PUBLISH_TIMEOUT_SEC = 10;

  private static final String ZOOKEEPER_DATA_DIR = Files.createTempDir().getAbsolutePath();

  private static final String ZOOKEEPER_HOST = "localhost";
  private static final int ZOOKEEPER_PORT = 2182;

  private static final String REDIS_HOST = "localhost";
  private static final int REDIS_PORT = 6380;

  // No of samples of feature row that will be generated and used for testing.
  // Note that larger no of samples will increase completion time for ingestion.
  private static final int IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE = 128;

  @Rule public TemporaryFolder deltaFolder = new TemporaryFolder();

  @Rule public TemporaryFolder checkpointFolder = new TemporaryFolder();

  @Rule public TemporaryFolder deadLetterFolder = new TemporaryFolder();

  @Rule public final SparkSessionRule spark = new SparkSessionRule();

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    assumeJava8();
    LocalKafka.start(
        KAFKA_HOST,
        KAFKA_PORT,
        KAFKA_REPLICATION_FACTOR,
        true,
        ZOOKEEPER_HOST,
        ZOOKEEPER_PORT,
        ZOOKEEPER_DATA_DIR);
    LocalRedis.start(REDIS_PORT);
  }

  @AfterClass
  public static void tearDown() {
    LocalRedis.stop();
    LocalKafka.stop();
  }

  private static void assumeJava8() {
    // Spark 2 only runs on Java 8. Skip tests on Java 11.
    Assume.assumeThat(System.getProperty("java.version"), startsWith("1.8"));
  }

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
    List<FeatureRow> inputForDelta =
        generateTestData(specForDelta, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);
    List<FeatureRow> invalidInput =
        generateTestData(invalidSpec, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);
    List<Pair<String, FeatureRow>> allInputs =
        Stream.concat(
                Stream.concat(inputForRedis.stream(), inputForDelta.stream()),
                invalidInput.stream())
            .map(f -> Pair.of("dummy", f))
            .collect(Collectors.toList());

    LOGGER.info("Starting Import Job");

    RedisConfig redisConfig =
        RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();
    Store redis = createStore(specForRedis, StoreType.REDIS).setRedisConfig(redisConfig).build();

    File deltaPath = deltaFolder.getRoot();
    DeltaConfig deltaConfig = DeltaConfig.newBuilder().setPath(deltaPath.getAbsolutePath()).build();
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
        featureSets.stream().map(s -> s.getSpec()).collect(Collectors.toList());

    Dataset<Row> data = null;

    String checkpointDir = checkpointFolder.getRoot().getAbsolutePath();
    String deadLetterDir = deadLetterFolder.getRoot().getAbsolutePath();

    SparkIngestion ingestion =
        new SparkIngestion(
            new String[] {
              TEST_JOB_ID,
              specsStreamingUpdateConfigJson,
              checkpointDir,
              "myDefaultFeastProject",
              deadLetterDir,
              storesJson,
              sourceJson
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

      String deltaTablePath = SparkDeltaSink.getDeltaTablePath(deltaPath.toString(), specForDelta);
      File deltaDirectory = new File(deltaTablePath);

      try {
        for (int i = 0; i < 120; i++) {
          if (Files.isDirectory().apply(deltaDirectory)) {
            data = spark.session.read().format("delta").load(deadLetterDir.toString());
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
        Arrays.stream(spark.session.streams().active()).forEach(q -> q.stop());
      }

      assertThat("Should have returned data", data, notNullValue());
      assertThat("Should have returned data", data.count(), greaterThan(0L));

      validateRedis(featureSetForRedis, inputForRedis, redisConfig, TEST_JOB_ID);

      validateDelta(featureSetForDelta, inputForDelta, deltaTablePath);

      validateDeadLetter(invalidInput);
    } finally {
      ingestion.stop = true;
      startup.join();
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

    Dataset<Row> data = spark.session.read().format("delta").load(deltaTablePath.toString());

    Set<FeatureRow> delta =
        data.limit(IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE + 1).collectAsList().stream()
            .map(row -> sparkRowToFeatureRow(featureSet.getSpec(), row))
            .collect(Collectors.toSet());

    // Ensure each of the retrieved FeatureRow is equal to the ingested FeatureRow.
    assertThat(delta.size(), is(input.size()));
    assertEquals(new HashSet<>(input), delta);
  }

  private void validateDeadLetter(List<FeatureRow> invalidInput) throws Exception {
    String deadLetterDir = deadLetterFolder.getRoot().getAbsolutePath();
    for (int i = 0; i < 60; i++) {

      Dataset<Row> data = spark.session.read().format("delta").load(deadLetterDir.toString());
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

    Dataset<Row> data = spark.session.read().format("delta").load(deadLetterDir.toString());
    long count = data.count();
    assertThat(count, is((long) IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE));
    Row f = data.first();
    assertThat(f.length(), is(6));
    int i = 0;
    assertThat("timestamp", f.get(i++), instanceOf(java.sql.Timestamp.class));
    assertThat("jobName", (String) f.getAs(i++), equalTo(""));
    assertThat("transformName", (String) f.getAs(i++), is("ValidateFeatureRow"));
    assertThat("payload", (String) f.getAs(i++), startsWith("fields"));
    assertThat(
        "errorMessage",
        (String) f.getAs(i++),
        containsString("FeatureRow contains invalid feature set id"));
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

  public final class SparkSessionRule implements TestRule {
    public SparkSession session;

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          assumeJava8();
          session =
              SparkSession.builder().appName(getClass().getName()).master("local").getOrCreate();
          try {
            base.evaluate(); // This will run the test.
          } finally {
            session.close();
          }
        }
      };
    }
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

    FeatureSet featureSet = FeatureSet.newBuilder().setSpec(spec1).build();
    return featureSet;
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
                      .map(field -> field.toBuilder().clearName().build())
                      .collect(Collectors.toList());
              randomRow =
                  randomRow
                      .toBuilder()
                      .clearFields()
                      .addAllFields(fields)
                      .clearFeatureSet()
                      .build();
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

  public static void validateRedis(
      FeatureSet featureSet, List<FeatureRow> input, RedisConfig redisConfig, String jobId) {

    Map<RedisKey, FeatureRow> expected = generateExpectedData(featureSet.getSpec(), input);

    LOGGER.info("Validating the actual values written to Redis ...");

    RedisURI redisuri =
        new RedisURI(
            redisConfig.getHost(), redisConfig.getPort(), java.time.Duration.ofMillis(2000));

    String password = redisConfig.getPass();
    if (StringUtils.trimToNull(password) != null) {
      redisuri.setPassword(password);
    }

    RedisClient redisClient = RedisClient.create(redisuri);
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
              LOGGER.info("Value: " + sync.get(randomKey));
            }
            Assert.fail("Missing key in Redis.");
          }

          // Ensure value is a valid serialized FeatureRow object.
          FeatureRow actualValue = null;
          try {
            actualValue = FeatureRow.parseFrom(actualByteValue);
          } catch (InvalidProtocolBufferException e) {
            Assert.fail(
                String.format(
                    "Actual Redis value cannot be parsed as FeatureRow, key: %s, value :%s",
                    key, new String(actualByteValue, StandardCharsets.UTF_8)));
          }

          // Ensure the retrieved FeatureRow is equal to the ingested FeatureRow.
          FeatureRow expectedValue1 =
              FeatureRow.newBuilder(expectedValue)
                  .setIngestionId(jobId)
                  .clearFields()
                  .addAllFields(
                      expectedValue.getFieldsList().stream()
                          .sorted(Comparator.comparing(Field::getName))
                          .collect(Collectors.toList()))
                  .build();
          Assert.assertEquals(expectedValue1, actualValue);
        });
    redisClient.shutdown();
  }
}
