package feast.serving.configuration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.Builder;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.FeastProperties;
import feast.serving.FeastProperties.JobProperties;
import feast.serving.service.BigQueryServingService;
import feast.serving.service.CachedSpecService;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisServingService;
import feast.serving.service.ServingService;
import io.opentracing.Tracer;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
@Configuration
public class ServingServiceConfig {

  @Bean(name = "JobStore")
  public Store jobStoreDefinition(FeastProperties feastProperties) {
    JobProperties jobProperties = feastProperties.getJobs();
    if (feastProperties.getJobs().getStoreType().equals("")) {
      return Store.newBuilder().build();
    }
    Map<String, String> options = jobProperties.getStoreOptions();
    Builder storeDefinitionBuilder =
        Store.newBuilder().setType(StoreType.valueOf(jobProperties.getStoreType()));
    return setStoreConfig(storeDefinitionBuilder, options);
  }

  private Store setStoreConfig(Store.Builder builder, Map<String, String> options) {
    switch (builder.getType()) {
      case REDIS:
        RedisConfig redisConfig =
            RedisConfig.newBuilder()
                .setHost(options.get("host"))
                .setPort(Integer.parseInt(options.get("port")))
                .build();
        return builder.setRedisConfig(redisConfig).build();
      case BIGQUERY:
        BigQueryConfig bqConfig =
            BigQueryConfig.newBuilder()
                .setProjectId(options.get("projectId"))
                .setDatasetId(options.get("datasetId"))
                .build();
        return builder.setBigqueryConfig(bqConfig).build();
      case CASSANDRA:
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported store %s provided, only REDIS or BIGQUERY are currently supported.",
                builder.getType()));
    }
  }

  @Bean
  public ServingService servingService(
      FeastProperties feastProperties,
      CachedSpecService specService,
      JobService jobService,
      Tracer tracer) {
    ServingService servingService = null;
    Store store = specService.getStore();

    switch (store.getType()) {
      case REDIS:
        RedisConfig redisConfig = store.getRedisConfig();
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(feastProperties.getStore().getRedisPoolMaxSize());
        poolConfig.setMaxIdle(feastProperties.getStore().getRedisPoolMaxIdle());
        JedisPool jedisPool =
            new JedisPool(poolConfig, redisConfig.getHost(), redisConfig.getPort());
        servingService = new RedisServingService(jedisPool, specService, tracer);
        break;
      case BIGQUERY:
        BigQueryConfig bqConfig = store.getBigqueryConfig();
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Storage storage = StorageOptions.getDefaultInstance().getService();
        String jobStagingLocation = feastProperties.getJobs().getStagingLocation();
        if (!jobStagingLocation.contains("://")) {
          throw new IllegalArgumentException(
              String.format("jobStagingLocation is not a valid URI: %s", jobStagingLocation));
        }
        if (jobStagingLocation.endsWith("/")) {
          jobStagingLocation = jobStagingLocation.substring(0, jobStagingLocation.length() - 1);
        }
        if (!jobStagingLocation.startsWith("gs://")) {
          throw new IllegalArgumentException(
              "Store type BIGQUERY requires job staging location to be a valid and existing Google Cloud Storage URI. Invalid staging location: "
                  + jobStagingLocation);
        }
        if (jobService.getClass() == NoopJobService.class) {
          throw new IllegalArgumentException(
              "Unable to instantiate jobService for BigQuery store.");
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
                "Unsupported store type '%s' for store name '%s'",
                store.getType(), store.getName()));
    }

    return servingService;
  }

  private Subscription parseSubscription(String subscription) {
    String[] split = subscription.split(":");
    return Subscription.newBuilder().setName(split[0]).setVersion(split[1]).build();
  }
}
