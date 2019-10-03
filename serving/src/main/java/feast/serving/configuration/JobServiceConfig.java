package feast.serving.configuration;

import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresRequest.Filter;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.serving.FeastProperties;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisBackedJobService;
import feast.serving.service.SpecService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Configuration
public class JobServiceConfig {
  private FeastProperties feastProperties;

  @Autowired
  public JobServiceConfig(FeastProperties feastProperties) {
    this.feastProperties = feastProperties;
  }

  @Bean
  public JobService jobService(SpecService specService) {
    String storeName = feastProperties.getStoreName();
    Store store = getStore(specService, storeName);
    if (store.getType() == StoreType.REDIS) {
      return new NoopJobService();
    }

    String jobStoreName = feastProperties.getJobStoreName();
    Store jobStore = getStore(specService, jobStoreName);
    StoreType storeType = jobStore.getType();
    JobService jobService = null;

    switch (storeType) {
      case REDIS:
        RedisConfig redisConfig = jobStore.getRedisConfig();
        Jedis jedis = new Jedis(redisConfig.getHost(), redisConfig.getPort());
        jobService = new RedisBackedJobService(jedis);
        break;
      case INVALID:
      case BIGQUERY:
      case CASSANDRA:
      case UNRECOGNIZED:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported store type '%s' for job store name '%s'", storeType, jobStoreName));
    }

    return jobService;
  }

  private Store getStore(SpecService specService, String jobStoreName) {
    GetStoresResponse storesResponse =
        specService.getStores(
            GetStoresRequest.newBuilder()
                .setFilter(Filter.newBuilder().setName(jobStoreName).build())
                .build());

    if (storesResponse.getStoreCount() < 1) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot resolve Store from store name '%s'. Ensure the store name exists in Feast.",
              jobStoreName));
    }

    assert storesResponse.getStoreCount() == 1;
    return storesResponse.getStore(0);
  }
}
