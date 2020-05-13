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
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.ingestion.options.BZip2Compressor;
import feast.ingestion.options.ImportOptions;
import feast.storage.RedisProto.RedisKey;
import feast.test.TestUtil;
import feast.test.TestUtil.LocalKafka;
import feast.test.TestUtil.LocalRedis;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto;
import feast.types.ValueProto.ValueType.Enum;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Assert;
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

  @Test
  public void runPipeline_ShouldWriteToRedisCorrectlyGivenValidSpecAndFeatureRow()
      throws IOException, InterruptedException {
    FeatureSetSpec spec =
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

    FeatureSet featureSet = FeatureSet.newBuilder().setSpec(spec).build();

    Store redis =
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

    ImportOptions options = PipelineOptionsFactory.create().as(ImportOptions.class);
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

    List<FeatureRow> input = new ArrayList<>();
    Map<RedisKey, FeatureRow> expected = new HashMap<>();

    LOGGER.info("Generating test data ...");
    IntStream.range(0, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE)
        .forEach(
            i -> {
              FeatureRow randomRow = TestUtil.createRandomFeatureRow(featureSet.getSpec());
              RedisKey redisKey = TestUtil.createRedisKey(featureSet.getSpec(), randomRow);
              input.add(randomRow);
              List<FieldProto.Field> fields =
                  randomRow.getFieldsList().stream()
                      .filter(
                          field ->
                              spec.getFeaturesList().stream()
                                  .map(FeatureSpec::getName)
                                  .collect(Collectors.toList())
                                  .contains(field.getName()))
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

    LOGGER.info("Starting Import Job with the following options: {}", options.toString());
    PipelineResult pipelineResult = ImportJob.runPipeline(options);
    Thread.sleep(Duration.standardSeconds(IMPORT_JOB_READY_DURATION_SEC).getMillis());
    Assert.assertEquals(pipelineResult.getState(), State.RUNNING);

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

    LOGGER.info("Validating the actual values written to Redis ...");
    RedisClient redisClient =
        RedisClient.create(new RedisURI(REDIS_HOST, REDIS_PORT, java.time.Duration.ofMillis(2000)));
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
          Assert.assertEquals(expectedValue, actualValue);
        });
    redisClient.shutdown();
  }
}
