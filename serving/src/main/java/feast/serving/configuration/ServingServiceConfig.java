package feast.serving.configuration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresRequest.Filter;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.serving.FeastProperties;
import feast.serving.service.BigQueryServingService;
import feast.serving.service.JobService;
import feast.serving.service.RedisServingService;
import feast.serving.service.ServingService;
import feast.serving.service.SpecService;
import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
@Configuration
public class ServingServiceConfig {
  private String feastStoreName;
  private String jobStagingLocation;

  @Autowired
  public ServingServiceConfig(FeastProperties feastProperties) {
    feastStoreName = feastProperties.getStoreName();
    jobStagingLocation = feastProperties.getJobStagingLocation();
  }

  @Bean
  public ServingService servingService(
      FeastProperties feastProperties,
      SpecService specService,
      JobService jobService,
      Tracer tracer) {
    if (feastStoreName.isEmpty()) {
      throw new IllegalArgumentException("Store name cannot be empty. Please provide a store name that has been registered in Feast.");
    }

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

    if (storesResponse.getStoreCount() > 1) {
      throw new IllegalArgumentException(
          String.format(
              "Store name '%s' resolves to more than 1 store in Feast. It should resolve to a unique store. Please check the store name configuration.",
              feastStoreName));
    }
    Store store = storesResponse.getStore(0);
    StoreType storeType = store.getType();
    ServingService servingService = null;

    switch (storeType) {
      case REDIS:
        RedisConfig redisConfig = store.getRedisConfig();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(feastProperties.getRedisPoolMaxSize());
        poolConfig.setMaxIdle(feastProperties.getRedisPoolMaxIdle());
        JedisPool jedisPool =
            new JedisPool(
                poolConfig, redisConfig.getHost(), redisConfig.getPort());
        servingService = new RedisServingService(jedisPool, specService, tracer);
        break;
      case BIGQUERY:
        BigQueryConfig bqConfig = store.getBigqueryConfig();
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        if (!jobStagingLocation.contains("://")) {
          throw new IllegalArgumentException(
              String.format("jobStagingLocation is not a valid URI: %s", jobStagingLocation));
        }
        if (jobStagingLocation.endsWith("/")) {
          jobStagingLocation = jobStagingLocation.substring(0, jobStagingLocation.length() - 1);
        }
        if (!this.jobStagingLocation.startsWith("gs://")) {
          throw new IllegalArgumentException(
              "Store type BIGQUERY requires job staging location to be a valid and existing Google Cloud Storage URI. Invalid staging location: "
                  + jobStagingLocation);
        }
        servingService =
            new BigQueryServingService(
                bigquery,
                bqConfig.getProjectId(),
                bqConfig.getDatasetId(),
                specService,
                jobService,
                jobStagingLocation,
                storage);
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
