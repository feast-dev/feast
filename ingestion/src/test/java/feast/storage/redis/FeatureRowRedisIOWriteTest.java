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

import static feast.storage.redis.FeatureRowToRedisMutationDoFn.getBucketId;
import static feast.storage.redis.FeatureRowToRedisMutationDoFn.getFeatureIdSha1Prefix;
import static feast.storage.redis.FeatureRowToRedisMutationDoFn.getRedisBucketKey;
import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import feast.ingestion.model.Features;
import feast.ingestion.model.Specs;
import feast.ingestion.model.SpecsImpl;
import feast.ingestion.model.Values;
import feast.ingestion.service.FileSpecService;
import feast.ingestion.transform.FeatureIO;
import feast.ingestion.util.DateUtil;
import feast.options.OptionsParser;
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
import org.joda.time.Duration;
import org.joda.time.format.ISOPeriodFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.Redis;
import redis.embedded.RedisServer;

public class FeatureRowRedisIOWriteTest {

  private static int REDIS_PORT = 51234;
  private static Redis redis;
  private static Specs specs;
  private static Jedis jedis;

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
    Path path = Paths.get(Resources.getResource("core_specs/").getPath());
    specs = new SpecsImpl(null, null, new FileSpecService.Builder(path.toString()));
    jedis = new Jedis("localhost", REDIS_PORT);
  }

  @AfterClass
  public static void teardown() throws IOException {
    redis.stop();
  }

  @Test
  public void testWriteNoneGranularity() throws IOException {
    String featureInt32 = "testEntity.none.redisInt32";
    String featureString = "testEntity.none.redisString";

    StorageSpec storageSpec =
        StorageSpec.newBuilder()
            .setId("redis1")
            .setType("redis")
            .putOptions("host", "localhost")
            .putOptions("port", String.valueOf(REDIS_PORT))
            .putOptions("batchSize", "1")
            .putOptions("timeout", "2000")
            .build();
    new RedisServingStore().create(storageSpec, specs);
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
                    .addFeatures(Features.of(featureInt32, Values.ofInt32(1)))
                    .addFeatures(Features.of(featureString, Values.ofString("a"))))
            .build();

    PCollection<FeatureRowExtended> input = testPipeline.apply(Create.of(rowExtended));

    input.apply("write to embedded redis", write);

    testPipeline.run();

    RedisBucketKey featureInt32Key =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureInt32), 0L);
    RedisBucketKey featureStringKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureString), 0L);

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
    String featureInt32 = "testEntity.none.redisInt32";
    String featureString = "testEntity.none.redisString";

    StorageSpec storageSpec =
        StorageSpec.newBuilder()
            .setId("redis1")
            .setType("redis")
            .putOptions("host", "localhost")
            .putOptions("port", String.valueOf(REDIS_PORT))
            .putOptions("batchSize", "1")
            .putOptions("timeout", "2000")
            .build();
    FeatureIO.Write write = new RedisServingStore().create(storageSpec, specs);

    FeatureRowExtended rowExtended =
        FeatureRowExtended.newBuilder()
            .setRow(
                FeatureRow.newBuilder()
                    .setEntityName("testEntity")
                    .setEntityKey("1")
                    .setGranularity(Granularity.Enum.NONE)
                    .setEventTimestamp(DateUtil.toTimestamp(DateTime.now()))
                    .addFeatures(Features.of(featureInt32, Values.ofInt32(1)))
                    .addFeatures(Features.of(featureString, Values.ofString("a"))))
            .build();

    PCollection<FeatureRowExtended> input = testPipeline.apply(Create.of(rowExtended));

    input.apply("write to embedded redis", write);

    testPipeline.run();

    RedisBucketKey featureInt32Key =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureInt32), 0L);
    RedisBucketKey featureStringKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureString), 0L);

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
    String featureInt32 = "testEntity.hour.redisInt32";
    String featureString = "testEntity.hour.redisString";

    FeatureRowRedisIO.Write write =
        new FeatureRowRedisIO.Write(
            RedisStoreOptions.builder().host("localhost").port(REDIS_PORT).build(), specs);

    FeatureRowExtended rowExtended =
        FeatureRowExtended.newBuilder()
            .setRow(
                FeatureRow.newBuilder()
                    .setEntityName("testEntity")
                    .setEntityKey("1")
                    .setGranularity(Granularity.Enum.HOUR)
                    .setEventTimestamp(DateUtil.toTimestamp(DateTime.now()))
                    .addFeatures(Features.of(featureInt32, Values.ofInt32(1)))
                    .addFeatures(Features.of(featureString, Values.ofString("a"))))
            .build();

    PCollection<FeatureRowExtended> input = testPipeline.apply(Create.of(rowExtended));

    input.apply("write to embedded redis", write);

    testPipeline.run();

    Timestamp rowTimestamp = rowExtended.getRow().getEventTimestamp();
    Timestamp roundedTimestamp = DateUtil.roundToGranularity(rowTimestamp, Granularity.Enum.HOUR);

    RedisBucketKey featureInt32LatestKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureInt32), 0L);
    RedisBucketKey featureStringLatestKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureString), 0L);

    RedisBucketKey featureInt32ValueKey =
        getRedisBucketKey("1", getFeatureIdSha1Prefix(featureInt32), roundedTimestamp.getSeconds());
    RedisBucketKey featureStringValueKey =
        getRedisBucketKey(
            "1", getFeatureIdSha1Prefix(featureString), roundedTimestamp.getSeconds());

    // TODO have a helper func for loading feature store options
    Duration featureInt32BucketSize =
        OptionsParser.parse(
                specs.getFeatureSpec(featureInt32).getDataStores().getServing().getOptionsMap(),
                RedisFeatureOptions.class)
            .getBucketSizeDuration();

    RedisBucketKey featureInt32BucketKey =
        getRedisBucketKey(
            "1",
            getFeatureIdSha1Prefix(featureInt32),
            getBucketId(roundedTimestamp, featureInt32BucketSize));
    RedisBucketKey featureStringBucketKey =
        getRedisBucketKey(
            "1",
            getFeatureIdSha1Prefix(featureString),
            // No bucketsize specified so uses the default.
            getBucketId(
                roundedTimestamp,
                ISOPeriodFormat.standard()
                    .parsePeriod(RedisFeatureOptions.DEFAULT_BUCKET_SIZE)
                    .toStandardDuration()));

    checkRedisValue(featureInt32LatestKey, Values.ofInt32(1), roundedTimestamp);
    checkRedisValue(featureStringLatestKey, Values.ofString("a"), roundedTimestamp);

    checkRedisValue(featureInt32ValueKey, Values.ofInt32(1), roundedTimestamp);
    checkRedisValue(featureStringValueKey, Values.ofString("a"), roundedTimestamp);

    checkRedisValue(featureInt32BucketKey, featureInt32ValueKey, roundedTimestamp);
    checkRedisValue(featureStringBucketKey, featureStringValueKey, roundedTimestamp);
  }

  /** Check that a key's value is another key. */
  void checkRedisValue(RedisBucketKey key, RedisBucketKey expectedValue, Timestamp timestamp)
      throws InvalidProtocolBufferException {
    Set<byte[]> result = jedis.zrangeByScore(key.toByteArray(), 0, Long.MAX_VALUE);
    assertEquals(1, result.size());
    RedisBucketKey value = RedisBucketKey.parseFrom(result.iterator().next());
    assertEquals(expectedValue, value);
  }

  void checkRedisValue(RedisBucketKey key, Value expectedValue, Timestamp expectedTimestamp)
      throws InvalidProtocolBufferException {
    RedisBucketValue featureInt32Value = RedisBucketValue.parseFrom(jedis.get(key.toByteArray()));
    assertEquals(expectedValue, featureInt32Value.getValue());
    assertEquals(expectedTimestamp, featureInt32Value.getEventTimestamp());
  }
}
