/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.store.serving.redis;

import static feast.store.serving.redis.FeatureRowToRedisMutationDoFn.getFeatureIdSha1Prefix;
import static feast.store.serving.redis.FeatureRowToRedisMutationDoFn.getRedisBucketKey;
import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import com.google.protobuf.Timestamp;
import feast.ingestion.config.ImportJobSpecsSupplier;
import feast.ingestion.model.Features;
import feast.ingestion.model.Specs;
import feast.ingestion.model.Values;
import feast.ingestion.util.DateUtil;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.RedisProto.RedisBucketKey;
import feast.storage.RedisProto.RedisBucketValue;
import feast.store.FeatureStoreWrite;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  private static final String featureInt32 = "testEntity.testInt32";
  private static final String featureString = "testEntity.testString";

  private static int REDIS_PORT = 51234;
  private static Redis redis;
  private static Jedis jedis;
  private static ImportJobSpecs importJobSpecs;

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @BeforeClass
  public static void classSetup() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
    Path path = Paths.get(Resources.getResource("specs/").getPath());
    importJobSpecs = new ImportJobSpecsSupplier(path.toString()).get();
    jedis = new Jedis("localhost", REDIS_PORT);
  }

  @AfterClass
  public static void teardown() {
    redis.stop();
  }

  Specs getSpecs() {
    Specs specs = Specs.of(
        "test job",
        importJobSpecs.toBuilder()
            .setSourceSpec(SourceSpec.newBuilder())
            .setEntitySpec(EntitySpec.newBuilder().setName("testEntity"))
            .addFeatureSpecs(FeatureSpec.newBuilder().setId(featureInt32))
            .addFeatureSpecs(FeatureSpec.newBuilder().setId(featureString))
            .setSinkStorageSpec(StorageSpec.newBuilder()
              .setId("REDIS1").setType("redis")
              .putOptions("port", String.valueOf(REDIS_PORT))
              .putOptions("host", "localhost")
              .putOptions("batchSize", "1")
              .putOptions("timeout", "2000")
              .build())
            .build());
    return specs;
  }

  @Test
  public void testWrite() throws IOException {

    Specs specs = getSpecs();
    new RedisServingFactory().create(specs.getSinkStorageSpec(), specs);
    FeatureRowRedisIO.Write write =
        new FeatureRowRedisIO.Write(
            RedisStoreOptions.builder().host("localhost").port(REDIS_PORT).build(), specs);

    Timestamp now = DateUtil.toTimestamp(DateTime.now());

    FeatureRowExtended rowExtended =
        FeatureRowExtended.newBuilder()
            .setRow(
                FeatureRow.newBuilder()
                    .setEntityName("testEntity")
                    .setEntityKey("1")
                    .setEventTimestamp(now)
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
    assertEquals(now, featureInt32Value.getEventTimestamp());
    assertEquals(Values.ofString("a"), featureStringValue.getValue());
    assertEquals(now, featureStringValue.getEventTimestamp());
  }

  @Test
  public void testWriteFromOptions() throws IOException {
    Specs specs = getSpecs();
    FeatureStoreWrite write = new RedisServingFactory()
        .create(specs.getSinkStorageSpec(), specs);

    Timestamp now = DateUtil.toTimestamp(DateTime.now());
    FeatureRowExtended rowExtended =
        FeatureRowExtended.newBuilder()
            .setRow(
                FeatureRow.newBuilder()
                    .setEntityName("testEntity")
                    .setEntityKey("1")
                    .setEventTimestamp(now)
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
    assertEquals(now, featureInt32Value.getEventTimestamp());
    assertEquals(Values.ofString("a"), featureStringValue.getValue());
    assertEquals(now, featureStringValue.getEventTimestamp());
  }
}
