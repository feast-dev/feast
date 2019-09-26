package feast.serving;

import com.google.protobuf.AbstractMessageLite;
import feast.storage.RedisProto.RedisKey;
import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.util.Lists;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTest {
  @Test
  public void shouldTest() {
    JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "10.202.120.41", 6379);
    List<RedisKey> keys = Lists.newArrayList(
        RedisKey.newBuilder().setFeatureSet("abc").build()
    );
    byte[][] bytes = keys.stream()
        .map(AbstractMessageLite::toByteArray)
        .collect(Collectors.toList())
        .toArray(new byte[0][0]);
    List<byte[]> out = jedisPool.getResource().mget(bytes);
    if (out.get(0) == null) {
      System.out.println("hi");
    }
  }
}
