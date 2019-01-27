/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.storage.redis;

import static feast.storage.redis.FeatureRowToRedisMutationDoFn.getFeatureIdSha1Prefix;
import static feast.storage.redis.FeatureRowToRedisMutationDoFn.getRedisBucketKey;
import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.ingestion.model.Features;
import feast.ingestion.model.Specs;
import feast.ingestion.model.Values;
import feast.ingestion.service.FileSpecService;
import feast.ingestion.service.SpecService;
import feast.ingestion.transform.FeatureIO;
import feast.ingestion.util.DateUtil;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity;
import feast.types.ValueProto.Value;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.Redis;
import redis.embedded.RedisServer;

public class FeatureRowRedisIOWriteTest {

  private static final String featureNoneInt32 = "testEntity.none.redisInt32";
  private static final String featureNoneString = "testEntity.none.redisString";
  private static final String featureHourInt32 = "testEntity.hour.redisInt32";
  private static final String featureHourString = "testEntity.hour.redisString";

  private static int REDIS_PORT = 51234;
  private static Redis redis;
  private static Jedis jedis;
  private static SpecService fileSpecService;

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @BeforeClass
  public static void classSetup() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
    Path path = Paths.get(Resources.getResource("core_specs/").getPath());
    fileSpecService = new FileSpecService.Builder(path.toString()).build();
    jedis = new Jedis("localhost", REDIS_PORT);
  }

  @AfterClass
  public static void teardown() {
    redis.stop();
  }

  Specs getSpecs() {
    Specs specs = Specs.of(
        "test job",
        ImportSpec.newBuilder()
            .addEntities("testEntity")
            .setSchema(
                Schema.newBuilder()
                    .addFields(Field.newBuilder().setFeatureId(featureHourInt32))
                    .addFields(Field.newBuilder().setFeatureId(featureHourString))
                    .addFields(Field.newBuilder().setFeatureId(featureNoneInt32))
                    .addFields(Field.newBuilder().setFeatureId(featureNoneString)))
            .build(),
        fileSpecService);
    StorageSpec redisSpec = specs.getStorageSpec("REDIS1");
    specs.getStorageSpecs().put("REDIS1",
        redisSpec.toBuilder().putOptions("port", String.valueOf(REDIS_PORT)).build());
    specs.validate();
    return specs;
  }

  @Test
  public void testWriteNoneGranularity() throws IOException {

    Specs specs = getSpecs();
    specs.validate();
    new RedisServingStore().create(specs.getStorageSpec("REDIS1"), specs);
    FeatureRowRedisIO.Write write =
        new FeatureRowRedisIO.Write(
            RedisStoreOptions.builder().host("localhost").port(REDIS_PORT).build(), specs);

    FeatureRowExtended rowExtended =
        FeatureRowExtended.newBuilder()
            .setRow(
                FeatureRow.newBuilder()
                    .setEntityName("testEntity")
                    .setEntityKey("1")
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(DateUtil.toTimestamp(DateTime.now()))
                    .addFeatures(Features.of(featureNoneInt32, Values.ofInt32(1)))
                    .addFeatures(Features.of(featureNoneString, Values.ofString("a"))))
            .build();

    PCollection<FeatureRowExtended> input = testPipeline.apply(Create.of(rowExtended));

    input.apply("write to embedded redis", write);

    testPipeline.run();

    RedisBucketKey featureInt32Key =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureNoneInt32), 0L);
    RedisBucketKey featureStringKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureNoneString), 0L);

    RedisBucketValue featureInt32Value =
        RedisBucketValue.parseFrom(jedis.get(featureInt32Key.toByteArray()));
    RedisBucketValue featureStringValue =
        RedisBucketValue.parseFrom(jedis.get(featureStringKey.toByteArray()));

    assertEquals(Values.ofInt32(1), featureInt32Value.getValue());
    // Timestamp is 0 for NONE granularity
    assertEquals(Timestamp.getDefaultInstance(), featureInt32Value.getEventTimestamp());
    assertEquals(Values.ofString("a"), featureStringValue.getValue());
    // Timestamp is 0 for NONE granularity
    assertEquals(Timestamp.getDefaultInstance(), featureStringValue.getEventTimestamp());
  }

  @Test
  public void testWriteNoneGranularityFromOptions() throws IOException {
    Specs specs = getSpecs();
    FeatureIO.Write write = new RedisServingStore().create(specs.getStorageSpec("REDIS1"), specs);

    FeatureRowExtended rowExtended =
        FeatureRowExtended.newBuilder()
            .setRow(
                FeatureRow.newBuilder()
                    .setEntityName("testEntity")
                    .setEntityKey("1")
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(DateUtil.toTimestamp(DateTime.now()))
                    .addFeatures(Features.of(featureNoneInt32, Values.ofInt32(1)))
                    .addFeatures(Features.of(featureNoneString, Values.ofString("a"))))
            .build();

    PCollection<FeatureRowExtended> input = testPipeline.apply(Create.of(rowExtended));

    input.apply("write to embedded redis", write);

    testPipeline.run();

    RedisBucketKey featureInt32Key =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureNoneInt32), 0L);
    RedisBucketKey featureStringKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureNoneString), 0L);

    RedisBucketValue featureInt32Value =
        RedisBucketValue.parseFrom(jedis.get(featureInt32Key.toByteArray()));
    RedisBucketValue featureStringValue =
        RedisBucketValue.parseFrom(jedis.get(featureStringKey.toByteArray()));

    assertEquals(Values.ofInt32(1), featureInt32Value.getValue());
    // Timestamp is 0 for NONE granularity
    assertEquals(Timestamp.getDefaultInstance(), featureInt32Value.getEventTimestamp());
    assertEquals(Values.ofString("a"), featureStringValue.getValue());
    // Timestamp is 0 for NONE granularity
    assertEquals(Timestamp.getDefaultInstance(), featureStringValue.getEventTimestamp());
  }

  @Test
  public void testWriteHourGranularity() throws IOException {
    Specs specs = getSpecs();
    FeatureIO.Write write = new RedisServingStore().create(specs.getStorageSpec("REDIS1"), specs);

    FeatureRowExtended rowExtended =
        FeatureRowExtended.newBuilder()
            .setRow(
                FeatureRow.newBuilder()
                    .setEntityName("testEntity")
                    .setEntityKey("1")
                    .setGranularity(Granularity.Enum.HOUR)
                    .setEventTimestamp(DateUtil.toTimestamp(DateTime.now()))
                    .addFeatures(Features.of(featureHourInt32, Values.ofInt32(1)))
                    .addFeatures(Features.of(featureHourString, Values.ofString("a"))))
            .build();

    PCollection<FeatureRowExtended> input = testPipeline.apply(Create.of(rowExtended));

    input.apply("write to embedded redis", write);

    testPipeline.run();

    Timestamp rowTimestamp = rowExtended.getRow().getEventTimestamp();
    Timestamp roundedTimestamp = DateUtil.roundToGranularity(rowTimestamp, Granularity.Enum.HOUR);

    RedisBucketKey featureInt32LatestKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureHourInt32), 0L);
    RedisBucketKey featureStringLatestKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureHourString), 0L);

    checkRedisValue(featureInt32LatestKey, Values.ofInt32(1), roundedTimestamp);
    checkRedisValue(featureStringLatestKey, Values.ofString("a"), roundedTimestamp);
  }

  void checkRedisValue(RedisBucketKey key, Value expectedValue, Timestamp expectedTimestamp)
      throws InvalidProtocolBufferException {
    RedisBucketValue featureInt32Value = RedisBucketValue.parseFrom(jedis.get(key.toByteArray()));
    assertEquals(expectedValue, featureInt32Value.getValue());
    assertEquals(expectedTimestamp, featureInt32Value.getEventTimestamp());
  }
}
