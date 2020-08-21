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

import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.util.JsonFormat;
import feast.ingestion.options.ImportOptions;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source;
import feast.proto.core.SourceProto.SourceType;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.RedisConfig;
import feast.proto.core.StoreProto.Store.StoreType;
import feast.proto.core.StoreProto.Store.Subscription;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto;
import feast.proto.types.ValueProto.ValueType.Enum;
import feast.test.TestUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

public class ImportJobTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportJobTest.class.getName());

  @ClassRule public static KafkaContainer kafkaContainer = new KafkaContainer();

  @ClassRule
  public static GenericContainer redisContainer =
      new GenericContainer("redis:5.0.3-alpine").withExposedPorts(6379);

  private static final String KAFKA_TOPIC = "topic_1";
  private static final String KAFKA_SPECS_TOPIC = "topic_specs_1";
  private static final String KAFKA_SPECS_ACK_TOPIC = "topic_specs_ack_1";

  private static final long KAFKA_PUBLISH_TIMEOUT_SEC = 10;

  // No of samples of feature row that will be generated and used for testing.
  // Note that larger no of samples will increase completion time for ingestion.
  private static final int IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE = 128;
  // Expected time taken for the import job to be ready to receive Feature Row input.
  private static final int IMPORT_JOB_READY_DURATION_SEC = 10;
  // The interval between checks for import job to finish writing elements to store.
  private static final int IMPORT_JOB_CHECK_INTERVAL_DURATION_SEC = 5;
  // Max duration to wait until the import job finishes writing to Store.
  private static final int IMPORT_JOB_MAX_RUN_DURATION_SEC = 300;

  @Test
  public void runPipeline_ShouldWriteToRedisCorrectlyGivenValidSpecAndFeatureRow()
      throws IOException, InterruptedException {
    Source featureSource =
        Source.newBuilder()
            .setType(SourceType.KAFKA)
            .setKafkaSourceConfig(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(kafkaContainer.getBootstrapServers())
                    .setTopic(KAFKA_TOPIC)
                    .build())
            .build();

    IngestionJobProto.SpecsStreamingUpdateConfig specsStreamingUpdateConfig =
        IngestionJobProto.SpecsStreamingUpdateConfig.newBuilder()
            .setSource(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(kafkaContainer.getBootstrapServers())
                    .setTopic(KAFKA_SPECS_TOPIC)
                    .build())
            .setAck(
                KafkaSourceConfig.newBuilder()
                    .setBootstrapServers(kafkaContainer.getBootstrapServers())
                    .setTopic(KAFKA_SPECS_ACK_TOPIC)
                    .build())
            .build();

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
            .setSource(featureSource)
            .build();

    FeatureSet featureSet = FeatureSet.newBuilder().setSpec(spec).build();

    Store redis =
        Store.newBuilder()
            .setName(StoreType.REDIS.toString())
            .setType(StoreType.REDIS)
            .setRedisConfig(
                RedisConfig.newBuilder()
                    .setHost(redisContainer.getHost())
                    .setPort(redisContainer.getFirstMappedPort())
                    .build())
            .addSubscriptions(
                Subscription.newBuilder()
                    .setProject(spec.getProject())
                    .setName(spec.getName())
                    .build())
            .build();

    ImportOptions options = PipelineOptionsFactory.create().as(ImportOptions.class);

    options.setSpecsStreamingUpdateConfigJson(
        JsonFormat.printer().print(specsStreamingUpdateConfig));
    options.setSourceJson(JsonFormat.printer().print(featureSource));
    options.setStoresJson(Collections.singletonList(JsonFormat.printer().print(redis)));
    options.setDefaultFeastProject("myproject");
    options.setProject("");
    options.setBlockOnRun(false);

    List<Pair<String, FeatureRow>> input = new ArrayList<>();
    Map<RedisKey, FeatureRow> expected = new HashMap<>();

    LOGGER.info("Generating test data ...");
    IntStream.range(0, IMPORT_JOB_SAMPLE_FEATURE_ROW_SIZE)
        .forEach(
            i -> {
              FeatureRow randomRow = TestUtil.createRandomFeatureRow(featureSet.getSpec());
              RedisKey redisKey = TestUtil.createRedisKey(featureSet.getSpec(), randomRow);
              input.add(Pair.of("", randomRow));
              List<FieldProto.Field> fields =
                  randomRow.getFieldsList().stream()
                      .filter(
                          field ->
                              spec.getFeaturesList().stream()
                                  .map(FeatureSpec::getName)
                                  .collect(Collectors.toList())
                                  .contains(field.getName()))
                      .map(
                          field ->
                              field.toBuilder().setName(TestUtil.hash(field.getName())).build())
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
    assertThat(pipelineResult.getState(), equalTo(State.RUNNING));

    LOGGER.info("Publishing {} Feature Row messages to Kafka ...", input.size());
    TestUtil.publishToKafka(
        kafkaContainer.getBootstrapServers(),
        KAFKA_SPECS_TOPIC,
        ImmutableList.of(Pair.of(getFeatureSetStringRef(spec), spec)),
        ByteArraySerializer.class,
        KAFKA_PUBLISH_TIMEOUT_SEC);
    TestUtil.publishToKafka(
        kafkaContainer.getBootstrapServers(),
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
        RedisClient.create(
            new RedisURI(
                redisContainer.getHost(),
                redisContainer.getFirstMappedPort(),
                java.time.Duration.ofMillis(2000)));
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    RedisCommands<byte[], byte[]> sync = connection.sync();
    for (Map.Entry<RedisKey, FeatureRow> entry : expected.entrySet()) {
      RedisKey key = entry.getKey();
      FeatureRow expectedValue = entry.getValue();

      // Ensure ingested key exists.
      byte[] actualByteValue = sync.get(key.toByteArray());
      assertThat("Key not found in Redis: " + key, actualByteValue, notNullValue());

      // Ensure value is a valid serialized FeatureRow object.
      FeatureRow actualValue = null;
      actualValue = FeatureRow.parseFrom(actualByteValue);

      // Ensure the retrieved FeatureRow is equal to the ingested FeatureRow.
      assertThat(actualValue, equalTo(expectedValue));
    }
    redisClient.shutdown();
  }
}
