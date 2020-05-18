/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.ingestion;

import com.google.common.io.Files;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.ingestion.options.BZip2Compressor;
import feast.ingestion.options.ImportOptions;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.test.TestUtil;
import feast.test.TestUtil.LocalKafka;
import feast.test.TestUtil.LocalRedis;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportJobTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportJobTest.class.getName());

  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 19092;
  private static final String KAFKA_BOOTSTRAP_SERVERS = KAFKA_HOST + ":" + KAFKA_PORT;
  private static final short KAFKA_REPLICATION_FACTOR = 1;
  private static final String KAFKA_TOPIC = "topic_1";
  private static final long KAFKA_PUBLISH_TIMEOUT_SEC = 10;

  @SuppressWarnings("UnstableApiUsage")
  private static final String ZOOKEEPER_DATA_DIR = Files.createTempDir().getAbsolutePath();

  private static final String ZOOKEEPER_HOST = "localhost";
  private static final int ZOOKEEPER_PORT = 2182;

  private static final String REDIS_HOST = "localhost";
  private static final int REDIS_PORT = 6380;

  // No of samples of feature row that will be generated and used for testing.
  // Note that larger no of samples will increase completion time for ingestion.
  private static final int IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE = 128;
  // Expected time taken for the import job to be ready to receive Feature Row input.
  private static final int IMPORT_JOB_READY_DURATION_SEC = 10;
  // The interval between checks for import job to finish writing elements to store.
  private static final int IMPORT_JOB_CHECK_INTERVAL_DURATION_SEC = 5;
  // Max duration to wait until the import job finishes writing to Store.
  private static final int IMPORT_JOB_MAX_RUN_DURATION_SEC = 300;
  private static final int FAILED_JOB_SAMPLE_FEATURE_ROW_SIZE = 5;

  private List<FeatureRow> defaultValues = new ArrayList<>();

  private FeatureSetSpec spec;
  private FeatureSet featureSet;
  private Store redis;
  private ImportOptions options;

  private RedisClient redisClient =
      RedisClient.create(new RedisURI(REDIS_HOST, REDIS_PORT, java.time.Duration.ofMillis(2000)));

  @Before
  public void setupFixtures() throws IOException, ClassNotFoundException {
    spec =
        FeatureSetSpec.newBuilder()
            .setName("feature_set")
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
            .addFeatures(
                FeatureSpec.newBuilder()
                    .setName("feature_1")
                    .setValueType(Enum.STRING_LIST)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_2").setValueType(Enum.STRING).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_3").setValueType(Enum.INT64).build())
            .setSource(
                Source.newBuilder()
                    .setType(SourceType.KAFKA)
                    .setKafkaSourceConfig(
                        KafkaSourceConfig.newBuilder()
                            .setBootstrapServers(KAFKA_HOST + ":" + KAFKA_PORT)
                            .setTopic(KAFKA_TOPIC)
                            .build())
                    .build())
            .build();

    ObjectInputStream in =
        new ObjectInputStream(
            new FileInputStream("src/test/resources/feast/ingestion/TestFeatureRows.txt"));

    List<FeatureRow> byteList = (List<FeatureRow>) in.readObject();
    in.close();

    for (FeatureRow bytes : byteList) {
      defaultValues.add(bytes);
    }

    featureSet = FeatureSet.newBuilder().setSpec(spec).build();

    redis =
        Store.newBuilder()
            .setName(StoreType.REDIS.toString())
            .setType(StoreType.REDIS)
            .setRedisConfig(
                RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build())
            .addSubscriptions(
                Subscription.newBuilder()
                    .setProject(spec.getProject())
                    .setName(spec.getName())
                    .build())
            .build();

    options = PipelineOptionsFactory.create().as(ImportOptions.class);
    BZip2Compressor<FeatureSetSpec> compressor =
        new BZip2Compressor<>(
            option -> {
              JsonFormat.Printer printer =
                  JsonFormat.printer().omittingInsignificantWhitespace().printingEnumsAsInts();
              return printer.print(option).getBytes();
            });
    options.setFeatureSetJson(compressor.compress(spec));
    options.setStoreJson(Collections.singletonList(JsonFormat.printer().print(redis)));
    options.setProject("");
    options.setBlockOnRun(false);
  }

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
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
  // Ingestion test where feature rows match registered spec exactly.
  @Test
  public void runPipeline_ShouldWriteToRedisCorrectlyGivenValidSpecAndFeatureRow()
      throws IOException, InterruptedException {
    List<FeatureRow> input = defaultValues.subList(0, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE);

    Map<RedisKey, FeatureRow> expected = getExpectedRedisMapping(input, featureSet.getSpec());
    PipelineResult pipelineResult = startImportJob(options);
    Assert.assertEquals(pipelineResult.getState(), State.RUNNING);
    publishFeatureRows(input, pipelineResult);

    LOGGER.info("Validating the actual values written to Redis ...");
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    RedisCommands<byte[], byte[]> sync = connection.sync();
    expected.forEach(
        (key, expectedValue) -> {

          // Ensure ingested key exists.
          byte[] actualByteValue = getBytesFromKey(sync, key);
          if (actualByteValue == null) {
            Assert.fail("Missing key in Redis.");
          }

          // Ensure value is a valid serialized FeatureRow object.
          FeatureRow actualValue = getFeatureRowFromByte(key, actualByteValue);

          // Ensure the retrieved FeatureRow is equal to the ingested FeatureRow.
          Assert.assertEquals(expectedValue, actualValue);
        });
    redisClient.shutdown();
  }

  /* Feature row contains an extra feature, not registered with store. This test verifies that
    these features are ignored in ingestion.
  */

  @Test
  public void runPipeline_ShouldWriteToRedisCorrectlyGivenUnknownFeatureFields()
      throws IOException, InterruptedException {

    List<FeatureRow> input =
        defaultValues.subList(0, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE).stream()
            .map(
                row ->
                    row.toBuilder()
                        .clone()
                        .addFields(
                            3,
                            Field.newBuilder()
                                .setName("feature_4")
                                .setValue(Value.newBuilder().setInt32Val(0).build()))
                        .build())
            .collect(Collectors.toList());

    Map<RedisKey, FeatureRow> expected = getExpectedRedisMapping(input, spec);

    PipelineResult pipelineResult = startImportJob(options);
    Assert.assertEquals(pipelineResult.getState(), State.RUNNING);
    publishFeatureRows(input, pipelineResult);

    LOGGER.info("Validating the actual values written to Redis ...");
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    RedisCommands<byte[], byte[]> sync = connection.sync();
    expected.forEach(
        (key, expectedValue) -> {

          // Ensure ingested key exists.
          byte[] actualByteValue = getBytesFromKey(sync, key);
          if (actualByteValue == null) {
            Assert.fail("Missing key in Redis.");
          }

          // Ensure value is a valid serialized FeatureRow object.
          FeatureRow actualValue = getFeatureRowFromByte(key, actualByteValue);

          // Ensure the retrieved FeatureRow is equal to the ingested FeatureRow.
          Assert.assertEquals(expectedValue, actualValue);
        });
    redisClient.shutdown();
  }

  /*
   If there are missing fields in an incoming feature row, this test verifies that their values are set to null during ingestion.
  */

  @Test
  public void runPipeline_ShouldWriteToRedisCorrectlyGivenMissingFieldsInFeatureRow()
      throws IOException, InterruptedException {

    List<FeatureRow> input =
        defaultValues.subList(0, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE).stream()
            .map(
                row -> {
                  List<Field> selectedFields =
                      new ArrayList<>(defaultValues.get(0).getFieldsList());
                  selectedFields.remove(3);
                  return row.toBuilder().clone().clearFields().addAllFields(selectedFields).build();
                })
            .collect(Collectors.toList());

    Map<RedisKey, FeatureRow> expected = getExpectedRedisMapping(input, spec);
    PipelineResult pipelineResult = startImportJob(options);
    Assert.assertEquals(pipelineResult.getState(), State.RUNNING);
    publishFeatureRows(input, pipelineResult);

    LOGGER.info("Validating the actual values written to Redis ...");
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    RedisCommands<byte[], byte[]> sync = connection.sync();
    expected.forEach(
        (key, expectedValue) -> {

          // Ensure ingested key exists.
          byte[] actualByteValue = getBytesFromKey(sync, key);
          if (actualByteValue == null) {
            Assert.fail("Missing key in Redis.");
          }

          // Ensure value is a valid serialized FeatureRow object.
          FeatureRow actualValue = getFeatureRowFromByte(key, actualByteValue);

          // Ensure the retrieved FeatureRow is equal to the ingested FeatureRow.
          Assert.assertEquals(expectedValue, actualValue);
        });
    redisClient.shutdown();
  }

  /*
   If there are missing entity fields in an incoming feature row, the row should not be ingested.
  */

  @Test
  public void runPipeline_ShouldNotWriteFeatureRowsWithMissingEntityFields()
      throws IOException, InterruptedException {

    List<FeatureRow> input =
        defaultValues.subList(
            0, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE - FAILED_JOB_SAMPLE_FEATURE_ROW_SIZE);
    input.addAll(
        defaultValues
            .subList(
                IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE - FAILED_JOB_SAMPLE_FEATURE_ROW_SIZE,
                IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE)
            .stream()
            .map(
                row -> {
                  List<Field> fields =
                      row.getFieldsList().stream()
                          .filter(field -> !(field.getName().equals("entity_id_primary")))
                          .collect(Collectors.toList());
                  return row.toBuilder().clone().clearFields().addAllFields(fields).build();
                })
            .collect(Collectors.toList()));

    Map<RedisKey, FeatureRow> expected = getExpectedRedisMapping(input, featureSet.getSpec());
    PipelineResult pipelineResult = startImportJob(options);
    Assert.assertEquals(pipelineResult.getState(), State.RUNNING);
    publishFeatureRows(input, pipelineResult);

    LOGGER.info("Validating the actual values written to Redis ...");
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    RedisCommands<byte[], byte[]> sync = connection.sync();
    expected.forEach(
        (key, expectedValue) -> {

          // Ensure ingested key exists.
          byte[] actualByteValue = getBytesFromKey(sync, key);
          if (actualByteValue == null) {
            Assert.fail("Missing key in Redis.");
          }

          // Ensure value is a valid serialized FeatureRow object.
          FeatureRow actualValue = getFeatureRowFromByte(key, actualByteValue);

          // Ensure the retrieved FeatureRow is equal to the ingested FeatureRow.
          Assert.assertEquals(expectedValue, actualValue);
        });
    redisClient.shutdown();
  }

  // Provides the expected key-value pairs in the Redis store, for the given incoming input, and
  // registered feature set spec configuration.
  private Map<RedisKey, FeatureRow> getExpectedRedisMapping(
      List<FeatureRow> input, FeatureSetSpec spec) {
    Map<RedisKey, FeatureRow> expected = new HashMap<>();
    input.forEach(
        i -> {
          if (spec.getEntitiesList().stream()
              .allMatch(
                  entity ->
                      i.getFieldsList().stream()
                          .map(Field::getName)
                          .collect(Collectors.toSet())
                          .contains(entity.getName()))) {
            RedisKey redisKey = TestUtil.createRedisKey(spec, i);
            FeatureRow value = getExpectedFeatureRow(spec, i);
            expected.put(redisKey, value);
          }
        });
    return expected;
  }
  // Utility function to obtain the actual byte value from the redis store given the key.
  private byte[] getBytesFromKey(RedisCommands<byte[], byte[]> sync, RedisKey key) {
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
    }
    return actualByteValue;
  }
  // Utility function to parse the bytes in the redis store as a feature row.
  private FeatureRow getFeatureRowFromByte(RedisKey key, byte[] actualByteValue) {
    FeatureRow actualValue = null;
    try {
      actualValue = FeatureRow.parseFrom(actualByteValue);
    } catch (InvalidProtocolBufferException e) {
      Assert.fail(
          String.format(
              "Actual Redis value cannot be parsed as FeatureRow, key: %s, value :%s",
              key, new String(actualByteValue, StandardCharsets.UTF_8)));
    }
    return actualValue;
  }

  // Simulates behaviour of actual ingestion job based on the supplied registered spec
  // and feature row.
  // Additional features found in the feature rows not present in the spec are ignored.
  // Features registered in the spec, but not found in feature rows, are set to null in the output
  // row.
  private FeatureRow getExpectedFeatureRow(FeatureSetSpec spec, FeatureRow row) {
    Map<String, Value> nonNullFields = new HashMap<>();
    for (Field field : row.getFieldsList()) {
      nonNullFields.put(field.getName(), field.getValue());
    }
    List<Field> fields =
        spec.getFeaturesList().stream()
            .map(
                field -> {
                  if (nonNullFields.containsKey(field.getName())) {
                    return Field.newBuilder().setValue(nonNullFields.get(field.getName())).build();
                  } else {
                    return Field.newBuilder().setValue(Value.newBuilder().build()).build();
                  }
                })
            .collect(Collectors.toList());

    row = row.toBuilder().clearFields().addAllFields(fields).clearFeatureSet().build();
    return row;
  }

  private PipelineResult startImportJob(ImportOptions options)
      throws IOException, InterruptedException {
    LOGGER.info("Starting Import Job with the following options: {}", options.toString());
    PipelineResult pipelineResult = ImportJob.runPipeline(options);
    Thread.sleep(Duration.standardSeconds(IMPORT_JOB_READY_DURATION_SEC).getMillis());
    return pipelineResult;
  }

  private void publishFeatureRows(List<FeatureRow> input, PipelineResult pipelineResult)
      throws InterruptedException {
    LOGGER.info("Publishing {} Feature Row messages to Kafka ...", input.size());
    TestUtil.publishFeatureRowsToKafka(
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        input,
        ByteArraySerializer.class,
        KAFKA_PUBLISH_TIMEOUT_SEC);
    TestUtil.waitUntilAllElementsAreWrittenToStore(
        pipelineResult,
        Duration.standardSeconds(IMPORT_JOB_MAX_RUN_DURATION_SEC),
        Duration.standardSeconds(IMPORT_JOB_CHECK_INTERVAL_DURATION_SEC));
  }
}
