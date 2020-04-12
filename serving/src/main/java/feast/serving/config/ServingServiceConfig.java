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
package feast.serving.config;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.StoreProto;
import feast.serving.service.HistoricalServingService;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.OnlineServingService;
import feast.serving.service.ServingService;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.HistoricalRetriever;
import feast.storage.api.retriever.OnlineRetriever;
import feast.storage.connectors.bigquery.retriever.BigQueryHistoricalRetriever;
import feast.storage.connectors.redis.retriever.RedisOnlineRetriever;
import io.opentracing.Tracer;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServingServiceConfig {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ServingServiceConfig.class);

  @Bean
  public ServingService servingService(
      FeastProperties feastProperties,
      CachedSpecService specService,
      JobService jobService,
      Tracer tracer)
      throws InvalidProtocolBufferException {
    ServingService servingService = null;
    StoreProto.Store store = feastProperties.getStore().toProto();

    switch (store.getType()) {
      case REDIS:
        OnlineRetriever redisRetriever = new RedisOnlineRetriever(store.getRedisConfig());
        servingService = new OnlineServingService(redisRetriever, specService, tracer);
        break;
      case BIGQUERY:
        if (jobService.getClass() == NoopJobService.class) {
          throw new IllegalArgumentException(
              "Unable to instantiate JobService which is required by BigQueryHistoricalRetriever.");
        }
        HistoricalRetriever bqRetriever =
            BigQueryHistoricalRetriever.fromConfig(store.getBigqueryConfig());
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
}
