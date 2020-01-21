package feast.serving.service;

import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.embedded.RedisServer;

public class RedisBackedJobServiceTest {
  private static String REDIS_HOST = "localhost";
  private static int REDIS_PORT = 51235;
  private RedisServer redis;

  @Before
  public void setUp() throws IOException {
    redis = new RedisServer(REDIS_PORT);
    redis.start();
  }

  @After
  public void teardown() {
    redis.stop();
  }

  @Test
  public void shouldRecoverIfRedisConnectionIsLost() {
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(1);
    jedisPoolConfig.setMaxWaitMillis(10);
    JedisPool jedisPool = new JedisPool(jedisPoolConfig, REDIS_HOST, REDIS_PORT);
    RedisBackedJobService jobService = new RedisBackedJobService(jedisPool);
    jobService.get("does not exist");
    redis.stop();
    try {
      jobService.get("does not exist");
    } catch (Exception e) {
      // pass, this should fail, and return a broken connection to the pool
    }
    redis.start();
    jobService.get("does not exist");
  }
}