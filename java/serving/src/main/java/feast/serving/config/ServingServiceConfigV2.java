/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.protobuf.AbstractMessageLite;
import feast.serving.registry.LocalRegistryRepo;
import feast.serving.service.OnlineServingServiceV2;
import feast.serving.service.OnlineTransformationService;
import feast.serving.service.ServingServiceV2;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.CoreFeatureSpecRetriever;
import feast.serving.specs.FeatureSpecRetriever;
import feast.serving.specs.RegistryFeatureSpecRetriever;
import feast.storage.api.retriever.OnlineRetrieverV2;
import feast.storage.connectors.bigtable.retriever.BigTableOnlineRetriever;
import feast.storage.connectors.bigtable.retriever.BigTableStoreConfig;
import feast.storage.connectors.cassandra.retriever.CassandraOnlineRetriever;
import feast.storage.connectors.cassandra.retriever.CassandraStoreConfig;
import feast.storage.connectors.redis.retriever.*;
import io.opentracing.Tracer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class ServingServiceConfigV2 {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ServingServiceConfigV2.class);

  @Autowired private ApplicationContext context;

  @Bean
  @Lazy(true)
  public BigtableDataClient bigtableClient(FeastProperties feastProperties) throws IOException {
    BigTableStoreConfig config = feastProperties.getActiveStore().getBigtableConfig();
    String projectId = config.getProjectId();
    String instanceId = config.getInstanceId();

    return BigtableDataClient.create(
        BigtableDataSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .setAppProfileId(config.getAppProfileId())
            .build());
  }

  @Bean
  @Conditional(CoreCondition.class)
  public ServingServiceV2 servingServiceV2(
      FeastProperties feastProperties, CachedSpecService specService, Tracer tracer) {
    final ServingServiceV2 servingService;
    final FeastProperties.Store store = feastProperties.getActiveStore();

    OnlineRetrieverV2 retrieverV2;
    switch (store.getType()) {
      case REDIS_CLUSTER:
        RedisClientAdapter redisClusterClient =
            RedisClusterClient.create(store.getRedisClusterConfig());
        retrieverV2 = new OnlineRetriever(redisClusterClient, (AbstractMessageLite::toByteArray));
        break;
      case REDIS:
        RedisClientAdapter redisClient = RedisClient.create(store.getRedisConfig());
        retrieverV2 = new OnlineRetriever(redisClient, (AbstractMessageLite::toByteArray));
        break;
      case BIGTABLE:
        BigtableDataClient bigtableClient = context.getBean(BigtableDataClient.class);
        retrieverV2 = new BigTableOnlineRetriever(bigtableClient);
        break;
      case CASSANDRA:
        CassandraStoreConfig config = feastProperties.getActiveStore().getCassandraConfig();
        String connectionString = config.getConnectionString();
        String dataCenter = config.getDataCenter();
        String keySpace = config.getKeySpace();

        List<InetSocketAddress> contactPoints =
            Arrays.stream(connectionString.split(","))
                .map(String::trim)
                .map(cs -> cs.split(":"))
                .map(
                    hostPort -> {
                      int port = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : 9042;
                      return new InetSocketAddress(hostPort[0], port);
                    })
                .collect(Collectors.toList());

        CqlSession session =
            new CqlSessionBuilder()
                .addContactPoints(contactPoints)
                .withLocalDatacenter(dataCenter)
                .withKeyspace(keySpace)
                .build();
        retrieverV2 = new CassandraOnlineRetriever(session);
        break;
      default:
        throw new RuntimeException(
            String.format("Unable to identify online store type: %s", store.getType()));
    }

    final FeatureSpecRetriever featureSpecRetriever;
    log.info("Created CoreFeatureSpecRetriever");
    featureSpecRetriever = new CoreFeatureSpecRetriever(specService);

    final String transformationServiceEndpoint = feastProperties.getTransformationServiceEndpoint();
    final OnlineTransformationService onlineTransformationService =
        new OnlineTransformationService(transformationServiceEndpoint, featureSpecRetriever);

    servingService =
        new OnlineServingServiceV2(
            retrieverV2, tracer, featureSpecRetriever, onlineTransformationService);

    return servingService;
  }

  @Bean
  @Conditional(RegistryCondition.class)
  public ServingServiceV2 registryBasedServingServiceV2(
      FeastProperties feastProperties, Tracer tracer) {
    final ServingServiceV2 servingService;
    final FeastProperties.Store store = feastProperties.getActiveStore();

    OnlineRetrieverV2 retrieverV2;
    // TODO: Support more store types, and potentially use a plugin model here.
    switch (store.getType()) {
      case REDIS_CLUSTER:
        RedisClientAdapter redisClusterClient =
            RedisClusterClient.create(store.getRedisClusterConfig());
        retrieverV2 = new OnlineRetriever(redisClusterClient, new EntityKeySerializerV2());
        break;
      case REDIS:
        RedisClientAdapter redisClient = RedisClient.create(store.getRedisConfig());
        log.info("Created EntityKeySerializerV2");
        retrieverV2 = new OnlineRetriever(redisClient, new EntityKeySerializerV2());
        break;
      default:
        throw new RuntimeException(
            String.format(
                "Unable to identify online store type: %s for Regsitry Backed Serving Service",
                store.getType()));
    }

    final FeatureSpecRetriever featureSpecRetriever;
    log.info("Created RegistryFeatureSpecRetriever");
    log.info("Working Directory = " + System.getProperty("user.dir"));
    final LocalRegistryRepo repo = new LocalRegistryRepo(Paths.get(feastProperties.getRegistry()));
    featureSpecRetriever = new RegistryFeatureSpecRetriever(repo);

    final String transformationServiceEndpoint = feastProperties.getTransformationServiceEndpoint();
    final OnlineTransformationService onlineTransformationService =
        new OnlineTransformationService(transformationServiceEndpoint, featureSpecRetriever);

    servingService =
        new OnlineServingServiceV2(
            retrieverV2, tracer, featureSpecRetriever, onlineTransformationService);

    return servingService;
  }
}
