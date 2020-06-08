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

import static feast.ingestion.utils.SpecUtil.getFeatureSetReference;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.DeltaConfig;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FeatureRowProto.FeatureRow.Builder;
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
import feast.spark.ingestion.delta.FeatureRowToSparkRow;
import feast.test.TestUtil;
import feast.test.TestUtil.LocalKafka;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.AfterClass;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestionTest.class.getName());

  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 19092;
  private static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOST + ":" + KAFKA_PORT;
  private static final short KAFKA_REPLICATION_FACTOR = 1;
  private static final String KAFKA_TOPIC = "topic_1";
  private static final long KAFKA_PUBLISH_TIMEOUT_SEC = 10;

  private static final String ZOOKEEPER_DATA_DIR = Files.createTempDir().getAbsolutePath();

  private static final String ZOOKEEPER_HOST = "localhost";
  private static final int ZOOKEEPER_PORT = 2182;

  private static final String REDIS_HOST = "localhost";
  private static final int REDIS_PORT = 6380;

  // No of samples of feature row that will be generated and used for testing.
  // Note that larger no of samples will increase completion time for ingestion.
  private static final int IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE = 128;

  @Rule public TemporaryFolder folder = new TemporaryFolder();

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
  }

  @AfterClass
  public static void tearDown() {
    LocalKafka.stop();
  }

  private static void assumeJava8() {
    // Spark 2 only runs on Java 8. Skip tests on Java 11.
    Assume.assumeThat(System.getProperty("java.version"), startsWith("1.8"));
  }

  @Test
  public void streamingQueryShouldWriteKafkaPayloadAsDeltaLake() throws Exception {
    KafkaSourceConfig kafka =
        KafkaSourceConfig.newBuilder()
            .setBootstrapServers(KAFKA_HOST + ":" + KAFKA_PORT)
            .setTopic(KAFKA_TOPIC)
            .build();

    FeatureSet featureSetForRedis = TestUtil.createFeatureSetForRedis(kafka);
    FeatureSet featureSetForDelta = TestUtil.createFeatureSetForDelta(kafka);
    FeatureSetSpec specForRedis = featureSetForRedis.getSpec();
    FeatureSetSpec specForDelta = featureSetForDelta.getSpec();

    List<FeatureRow> inputForRedis =
        TestUtil.generateTestData(specForRedis, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);
    List<FeatureRow> inputForDelta =
        TestUtil.generateTestData(specForDelta, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);

    LOGGER.info("Starting Import Job");

    RedisConfig redisConfig =
        RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();
    Store redis =
        TestUtil.createStore(specForRedis, StoreType.REDIS).setRedisConfig(redisConfig).build();

    File deltaPath = folder.getRoot();
    DeltaConfig deltaConfig = DeltaConfig.newBuilder().setPath(deltaPath.getAbsolutePath()).build();
    Store delta =
        TestUtil.createStore(specForDelta, StoreType.DELTA).setDeltaConfig(deltaConfig).build();

    String storesJson = toJsonLines(Arrays.asList(delta, redis));
    List<FeatureSet> featureSets = Arrays.asList(featureSetForRedis, featureSetForDelta);
    List<FeatureSetSpec> featureSetSpecs =
        featureSets.stream().map(s -> s.getSpec()).collect(Collectors.toList());
    String featureSetsJson = toJsonLines(featureSetSpecs);

    Dataset<Row> data = null;

    SparkIngestion ingestion =
        new SparkIngestion(
            new String[] {"testjob", "myDefaultFeastProject", featureSetsJson, storesJson});

    StreamingQuery query = ingestion.createQuery();

    for (int i = 0; i < 60 && !query.status().isDataAvailable(); i++) {
      LOGGER.info("Waiting for trigger to start");
      Thread.sleep(1000);
    }

    describedAs("Should consume", is(true), query.status().isDataAvailable());

    LOGGER.info(
        "Publishing {} Feature Row messages to Kafka ...",
        inputForRedis.size() + inputForDelta.size());
    TestUtil.publishFeatureRowsToKafka(
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        Stream.concat(inputForRedis.stream(), inputForDelta.stream()).collect(Collectors.toList()),
        ByteArraySerializer.class,
        KAFKA_PUBLISH_TIMEOUT_SEC);

    try {
      for (int i = 0; i < 60; i++) {
        String deltaTablePath = ingestion.getDeltaTablePath(deltaPath.toString(), specForDelta);
        if (Files.isDirectory().apply(new File(deltaTablePath))) {
          data = spark.session.read().format("delta").load(deltaTablePath.toString());
          long count = data.count();
          LOGGER.info("Delta table contains {} records.", count);
          if (count >= IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE) {
            break;
          }
        } else {
          LOGGER.info("Delta directory not yet created.");
        }
        Thread.sleep(1000);
      }
    } finally {
      query.stop();
    }

    describedAs("Should have returned data", notNullValue(), data);
    describedAs("Should have returned data", greaterThan(0L), data.count());

    TestUtil.validateRedis(featureSetForRedis, inputForRedis, redisConfig);

    validateDelta(featureSetForDelta, inputForDelta, data);
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

  public static void validateDelta(
      FeatureSet featureSet, List<FeatureRow> input, Dataset<Row> data) {
    LOGGER.info("Validating the actual values written to Delta ...");

    Set<FeatureRow> delta =
        data.limit(IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE + 1).collectAsList().stream()
            .map(row -> sparkRowToFeatureRow(featureSet.getSpec(), row))
            .collect(Collectors.toSet());

    // Ensure each of the retrieved FeatureRow is equal to the ingested FeatureRow.
    assertThat(delta.size(), is(input.size()));
    assertEquals(new HashSet<>(input), delta);
  }

  public static FeatureRow sparkRowToFeatureRow(FeatureSetSpec featureSetSpec, Row row) {
    java.sql.Timestamp ts = row.getAs(FeatureRowToSparkRow.EVENT_TIMESTAMP_COLUMN);
    Builder builder =
        FeatureRow.newBuilder()
            .setFeatureSet(getFeatureSetReference(featureSetSpec))
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
}
