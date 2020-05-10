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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import feast.proto.core.StoreProto;
import feast.serving.service.HistoricalServingService;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.OnlineServingService;
import feast.serving.service.ServingService;
import feast.serving.specs.CachedSpecService;
import feast.storage.api.retriever.HistoricalRetriever;
import feast.storage.api.retriever.OnlineRetriever;
import feast.storage.connectors.bigquery.retriever.BigQueryHistoricalRetriever;
import feast.storage.connectors.redis.retriever.RedisClusterOnlineRetriever;
import feast.storage.connectors.redis.retriever.RedisOnlineRetriever;
import feast.storage.connectors.snowflake.retriever.SnowflakeHistoricalRetriever;
import feast.storage.connectors.snowflake.retriever.SnowflakeQueryTemplater;
import io.opentracing.Tracer;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class ServingServiceConfig {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ServingServiceConfig.class);

  @Bean
  public ServingService servingService(
      FeastProperties feastProperties,
      CachedSpecService specService,
      JobService jobService,
      Tracer tracer)
      throws InvalidProtocolBufferException, JsonProcessingException {
    ServingService servingService = null;
    FeastProperties.Store store = feastProperties.getActiveStore();
    StoreProto.Store.StoreType storeType = store.toProto().getType();
    Map<String, String> config = store.getConfig();

    switch (storeType) {
      case REDIS_CLUSTER:
        OnlineRetriever redisClusterRetriever = RedisClusterOnlineRetriever.create(config);
        servingService = new OnlineServingService(redisClusterRetriever, specService, tracer);
        break;
      case REDIS:
        OnlineRetriever redisRetriever = RedisOnlineRetriever.create(config);
        servingService = new OnlineServingService(redisRetriever, specService, tracer);
        break;
      case BIGQUERY:
        validateJobServicePresence(jobService);
        HistoricalRetriever bqRetriever = BigQueryHistoricalRetriever.create(config);
        servingService = new HistoricalServingService(bqRetriever, specService, jobService);
        break;
      case Snowflake:
        validateJobServicePresence(jobService);
        HistoricalRetriever snowflakeHistoricalRetriever =
            this.createSnowflakeHistoricalRetriever(feastProperties);
        servingService =
            new HistoricalServingService(snowflakeHistoricalRetriever, specService, jobService);
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

  public HistoricalRetriever createSnowflakeHistoricalRetriever(FeastProperties feastProperties) {
    FeastProperties.Store store = feastProperties.getActiveStore();
    Map<String, String> config = store.getConfig();
    String className = config.get("class_name");
    switch (className) {
      case "net.snowflake.client.jdbc.SnowflakeDriver":
        SnowflakeQueryTemplater snowflakeQueryTemplater =
            new SnowflakeQueryTemplater(config, this.createJdbcTemplate(feastProperties));
        return SnowflakeHistoricalRetriever.create(config, snowflakeQueryTemplater);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported JDBC store className '%s' for store name '%s'",
                className, store.getName()));
    }
  }

  public DataSource createSnowflakeDataSource(FeastProperties feastProperties) {
    FeastProperties.Store store = feastProperties.getActiveStore();
    Map<String, String> config = store.getConfig();
    Properties dsProperties = new Properties();
    dsProperties.put("user", config.get("username"));
    dsProperties.put("password", config.get("password"));
    dsProperties.put("db", config.get("database"));
    dsProperties.put("schema", config.get("schema"));
    dsProperties.put("role", config.get("role"));
    dsProperties.put("warehouse", config.get("warehouse"));
    HikariConfig hkConfig = new HikariConfig();
    hkConfig.setMaximumPoolSize(100);
    hkConfig.setDriverClassName(config.get("class_name"));
    hkConfig.setJdbcUrl(config.get("url"));
    hkConfig.setDataSourceProperties(dsProperties);
    return new HikariDataSource(hkConfig);
  }

  public JdbcTemplate createJdbcTemplate(FeastProperties feastProperties) {
    return new JdbcTemplate(this.createSnowflakeDataSource(feastProperties));
  }

  private void validateJobServicePresence(JobService jobService) {
    if (jobService.getClass() == NoopJobService.class) {
      throw new IllegalArgumentException(
          "Job service has not been instantiated. The Job service is required for all historical stores.");
    }
  }
}
