package feast.serving.configuration;

import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.serving.service.CachedSpecService;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisBackedJobService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;

@Configuration
public class JobServiceConfig {

  @Bean
  public JobService jobService(Store jobStore, CachedSpecService specService) {
    if (!specService.getStore().getType().equals(StoreType.BIGQUERY)) {
      return new NoopJobService();
    }

    switch (jobStore.getType()) {
      case REDIS:
        RedisConfig redisConfig = jobStore.getRedisConfig();
        Jedis jedis = new Jedis(redisConfig.getHost(), redisConfig.getPort());
        return new RedisBackedJobService(jedis);
      case INVALID:
      case BIGQUERY:
      case CASSANDRA:
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported store type '%s' for job store name '%s'",
                jobStore.getType(), jobStore.getName()));
    }
  }
}
