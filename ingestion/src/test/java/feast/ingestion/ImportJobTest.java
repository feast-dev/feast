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
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.ingestion.options.ImportOptions;
import feast.storage.RedisProto.RedisKey;
import feast.test.TestUtil;
import feast.test.TestUtil.LocalKafka;
import feast.test.TestUtil.LocalRedis;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.ValueType.Enum;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

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

  // Expected time taken for the import job to be ready to receive Feature Row input
  private static final int IMPORT_JOB_READY_DURATION_SEC = 5;
  // Expected time taken for the import job to finish writing to Store
  private static final int IMPORT_JOB_RUN_DURATION_SEC = 30;

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
            .setVersion(3)
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

    Store redis =
        Store.newBuilder()
            .setName(StoreType.REDIS.toString())
            .setType(StoreType.REDIS)
            .setRedisConfig(
                RedisConfig.newBuilder().setHost(REDIS_HOST).setPort(REDIS_PORT).build())
            .addSubscriptions(
                Subscription.newBuilder()
                    .setName(spec.getName())
                    .setVersion(String.valueOf(spec.getVersion()))
                    .build())
            .build();

    ImportOptions options = PipelineOptionsFactory.create().as(ImportOptions.class);
    options.setFeatureSetSpecJson(
        Collections.singletonList(
            JsonFormat.printer().omittingInsignificantWhitespace().print(spec)));
    options.setStoreJson(
        Collections.singletonList(
            JsonFormat.printer().omittingInsignificantWhitespace().print(redis)));
    options.setProject("");
    options.setBlockOnRun(false);

    int inputSize = 128;
    List<FeatureRow> input = new ArrayList<>();
    Map<RedisKey, FeatureRow> expected = new HashMap<>();

    LOGGER.info("Generating test data ...");
    IntStream.range(0, inputSize)
        .forEach(
            i -> {
              FeatureRow randomRow = TestUtil.createRandomFeatureRow(spec);
              RedisKey redisKey = TestUtil.createRedisKey(spec, randomRow);
              input.add(randomRow);
              expected.put(redisKey, randomRow);
            });

    LOGGER.info("Starting Import Job with the following options: {}", options.toString());
    PipelineResult pipelineResult = ImportJob.runPipeline(options);
    Thread.sleep(Duration.ofSeconds(IMPORT_JOB_READY_DURATION_SEC).toMillis());
    Assert.assertEquals(pipelineResult.getState(), State.RUNNING);

    LOGGER.info("Publishing {} Feature Row messages to Kafka ...", input.size());
    TestUtil.publishFeatureRowsToKafka(
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        input,
        ByteArraySerializer.class,
        KAFKA_PUBLISH_TIMEOUT_SEC);
    Thread.sleep(Duration.ofSeconds(IMPORT_JOB_RUN_DURATION_SEC).toMillis());

    LOGGER.info("Validating the actual values written to Redis ...");
    Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
    expected.forEach(
        (key, expectedValue) -> {
          byte[] actualByteValue = jedis.get(key.toByteArray());
          Assert.assertNotNull("Key not found in Redis: " + key, actualByteValue);
          FeatureRow actualValue = null;
          try {
            actualValue = FeatureRow.parseFrom(actualByteValue);
          } catch (InvalidProtocolBufferException e) {
            Assert.fail(
                String.format(
                    "Actual Redis value cannot be parsed as FeatureRow, key: %s, value :%s",
                    key, new String(actualByteValue, StandardCharsets.UTF_8)));
          }
          Assert.assertEquals(expectedValue, actualValue);
        });
  }
}
