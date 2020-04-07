/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.configuration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.FeastProperties;
import feast.serving.service.*;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.HistoricalRetriever;
import feast.storage.api.retriever.OnlineRetriever;
import feast.storage.connectors.bigquery.retriever.BigQueryHistoricalRetriever;
import feast.storage.connectors.redis.retriever.RedisOnlineRetriever;
import io.opentracing.Tracer;
import java.util.Map;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServingServiceConfig {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ServingServiceConfig.class);

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
      Tracer tracer,
      StoreConfiguration storeConfiguration) {
    ServingService servingService = null;
    Store store = specService.getStore();

    switch (store.getType()) {
      case REDIS:
        OnlineRetriever redisRetriever =
            new RedisOnlineRetriever(storeConfiguration.getServingRedisConnection());
        servingService = new OnlineServingService(redisRetriever, specService, tracer);
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

        HistoricalRetriever bqRetriever =
            BigQueryHistoricalRetriever.builder()
                .setBigquery(bigquery)
                .setDatasetId(bqConfig.getDatasetId())
                .setProjectId(bqConfig.getProjectId())
                .setJobStagingLocation(jobStagingLocation)
                .setInitialRetryDelaySecs(
                    feastProperties.getJobs().getBigqueryInitialRetryDelaySecs())
                .setTotalTimeoutSecs(feastProperties.getJobs().getBigqueryTotalTimeoutSecs())
                .setStorage(storage)
                .build();

        servingService = new HistoricalServingService(bqRetriever, specService, jobService);
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
