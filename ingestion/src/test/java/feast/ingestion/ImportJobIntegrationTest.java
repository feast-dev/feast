package feast.ingestion;

import org.junit.Test;
import redis.embedded.RedisServer;

import java.io.IOException;

public class ImportJobIntegrationTest {
  @Test
  public void importJobWithRedisStorage() throws IOException {
    RedisServer redisServer = null;

    try {
      redisServer = new RedisServer(6379);
      redisServer.start();
    } finally {
      if (redisServer != null) {
        redisServer.stop();
      }
    }
  }
}
