package feast.serving.configuration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresRequest.Filter;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.serving.FeastProperties;
import feast.serving.service.BigQueryServingService;
import feast.serving.service.RedisServingService;
import feast.serving.service.ServingService;
import feast.serving.service.SpecService;
import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
@Configuration
public class ServingServiceConfig {
  private String feastStoreName;

  @Autowired
  public ServingServiceConfig(FeastProperties feastProperties) {
    feastStoreName = feastProperties.getStoreName();
  }

  @Bean
  @DependsOn({"specService", "tracer"})
  public ServingService servingService(
      FeastProperties feastProperties,
      SpecService specService,
      Tracer tracer) {
    GetStoresResponse storesResponse =
        specService.getStores(
            GetStoresRequest.newBuilder()
                .setFilter(Filter.newBuilder().setName(feastStoreName).build())
                .build());
    if (storesResponse.getStoreCount() < 1) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot resolve Store from store name '%s'. Ensure the store name exists in Feast.",
              feastStoreName));
    }
    assert storesResponse.getStoreCount() == 1;
    Store store = storesResponse.getStore(0);
    StoreType storeType = store.getType();
    ServingService servingService = null;

    switch (storeType) {
      case REDIS:
        RedisConfig redisConfig = store.getRedisConfig();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(feastProperties.getRedisPoolMaxSize());
        poolConfig.setMaxIdle(feastProperties.getRedisPoolMaxIdle());
        JedisPool jedisPool = new JedisPool(poolConfig, store.getRedisConfig().getHost(),
            store.getRedisConfig().getPort());
        servingService = new RedisServingService(jedisPool, specService, tracer);
        break;
      case BIGQUERY:
        BigQueryConfig bqConfig = store.getBigqueryConfig();
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        servingService =
            new BigQueryServingService(bigquery, bqConfig.getProjectId(), bqConfig.getDatasetId());
        break;
      case CASSANDRA:
      case UNRECOGNIZED:
      case INVALID:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported store type '%s' for store name '%s'", storeType, feastStoreName));
    }

    return servingService;
  }
}
