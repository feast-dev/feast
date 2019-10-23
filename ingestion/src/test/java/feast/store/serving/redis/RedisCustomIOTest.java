package feast.store.serving.redis;

import static feast.test.TestUtil.field;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import feast.storage.RedisProto.RedisKey;
import feast.store.serving.redis.RedisCustomIO.Method;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.ValueType.Enum;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.Redis;
import redis.embedded.RedisServer;

public class RedisCustomIOTest {

  @Rule
  public transient TestPipeline p = TestPipeline.create();

  private static int REDIS_PORT = 51234;
  private static Redis redis;
  private static Jedis jedis;


  @BeforeClass
  public static void setUp() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
    jedis = new Jedis("localhost", REDIS_PORT);
  }

  @AfterClass
  public static void teardown() {
    redis.stop();
  }

  @Test
  public void shouldWriteToRedis() {
    HashMap<RedisKey, FeatureRow> kvs = new LinkedHashMap<>();
    kvs.put(RedisKey.newBuilder().setFeatureSet("fs:1")
            .addEntities(field("entity", 1, Enum.INT64)).build(),
        FeatureRow.newBuilder().setFeatureSet("fs:1")
            .addFields(field("entity", 1, Enum.INT64))
            .addFields(field("feature", "one", Enum.STRING)).build());
    kvs.put(RedisKey.newBuilder().setFeatureSet("fs:1")
            .addEntities(field("entity", 2, Enum.INT64)).build(),
        FeatureRow.newBuilder().setFeatureSet("fs:1")
            .addFields(field("entity", 2, Enum.INT64))
            .addFields(field("feature", "two", Enum.STRING)).build());

    List<RedisMutation> featureRowWrites = kvs.entrySet().stream()
        .map(kv -> RedisMutation.newBuilder()
            .setMethod(Method.SET)
            .setKey(kv.getKey().toByteArray())
            .setValue(kv.getValue().toByteArray())
            .build())
        .collect(Collectors.toList());

    p.apply(Create.of(featureRowWrites))
        .apply(RedisCustomIO.write("localhost", REDIS_PORT));
    p.run();

    kvs.entrySet().forEach(kv -> {
      byte[] actual = jedis.get(kv.getKey().toByteArray());
      assertThat(actual, equalTo(kv.getValue().toByteArray()));
    });
  }
}