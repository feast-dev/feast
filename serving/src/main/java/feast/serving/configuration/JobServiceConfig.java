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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.*;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.StoreType;
import feast.serving.FeastProperties;
import feast.serving.service.CassandraBackedJobService;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisBackedJobService;
import feast.serving.specs.CachedSpecService;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobServiceConfig {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JobServiceConfig.class);

  @Bean
  public JobService jobService(
      FeastProperties feastProperties,
      CachedSpecService specService,
      StoreConfiguration storeConfiguration) {
    if (!specService.getStore().getType().equals(StoreType.BIGQUERY)) {
      return new NoopJobService();
    }
    StoreType storeType = StoreType.valueOf(feastProperties.getJobs().getStoreType());
    switch (storeType) {
      case REDIS:
        return new RedisBackedJobService(storeConfiguration.getJobStoreRedisConnection());
      case INVALID:
      case BIGQUERY:
      case CASSANDRA:
        StoreProto.Store store = specService.getStore();
        StoreProto.Store.CassandraConfig cassandraConfig = store.getCassandraConfig();
        Map<String, String> storeOptions = feastProperties.getJobs().getStoreOptions();
        List<InetSocketAddress> contactPoints =
            Arrays.stream(storeOptions.get("bootstrapHosts").split(","))
                .map(h -> new InetSocketAddress(h, Integer.parseInt(storeOptions.get("port"))))
                .collect(Collectors.toList());
        CqlSession cluster = CqlSession.builder().addContactPoints(contactPoints).build();
        // Session in Cassandra is thread-safe and maintains connections to cluster nodes internally
        // Recommended to use one session per keyspace instead of open and close connection for each
        // request
        log.info(String.format("Cluster name: %s", cluster.getName()));
        log.info(String.format("Cluster keyspace: %s", cluster.getMetadata().getKeyspaces()));
        log.info(String.format("Cluster nodes: %s", cluster.getMetadata().getNodes()));
        log.info(String.format("Cluster tokenmap: %s", cluster.getMetadata().getTokenMap()));
        log.info(
            String.format(
                "Cluster default profile: %s",
                cluster.getContext().getConfig().getDefaultProfile().toString()));
        log.info(
            String.format(
                "Cluster lb policies: %s", cluster.getContext().getLoadBalancingPolicies()));
        // Session in Cassandra is thread-safe and maintains connections to cluster nodes internally
        // Recommended to use one session per keyspace instead of open and close connection for each
        // request
        try {
          String keyspace = storeOptions.get("keyspace");
          Optional<KeyspaceMetadata> keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
          if (keyspaceMetadata.isEmpty()) {
            log.info("Creating keyspace '{}'", keyspace);
            Map<String, Integer> replicationOptions = new HashMap<>();
            replicationOptions.put(
                storeOptions.get("replicationOptionsDc"),
                Integer.parseInt(storeOptions.get("replicationOptionsReplicas")));
            CreateKeyspace createKeyspace =
                SchemaBuilder.createKeyspace(keyspace)
                    .ifNotExists()
                    .withNetworkTopologyStrategy(replicationOptions);
            createKeyspace.withDurableWrites(true);
            cluster.execute(createKeyspace.asCql());
          }
          CreateTable createTable =
              SchemaBuilder.createTable(keyspace, storeOptions.get("tableName"))
                  .ifNotExists()
                  .withPartitionKey("job_uuid", DataTypes.TEXT)
                  .withClusteringColumn("timestamp", DataTypes.TIMESTAMP)
                  .withColumn("job_data", DataTypes.TEXT);
          log.info("Create Cassandra table if not exists..");
          cluster.execute(createTable.asCql());

        } catch (RuntimeException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to connect to Cassandra at bootstrap hosts: '%s' port: '%s'. Please check that your Cassandra is running and accessible from Feast.",
                  contactPoints.stream()
                      .map(InetSocketAddress::getHostName)
                      .collect(Collectors.joining(",")),
                  cassandraConfig.getPort()),
              e);
        }
        return new CassandraBackedJobService(cluster);
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported store type '%s' for job store", storeType));
    }
  }
}
