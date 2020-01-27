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

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.StoreType;
import feast.serving.FeastProperties;
import feast.serving.service.CassandraBackedJobService;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisBackedJobService;
import feast.serving.specs.CachedSpecService;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobServiceConfig {

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
<<<<<<< HEAD
        return new RedisBackedJobService(storeConfiguration.getJobStoreRedisConnection());
=======
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(
            Integer.parseInt(storeOptions.getOrDefault("max-conn", DEFAULT_REDIS_MAX_CONN)));
        jedisPoolConfig.setMaxIdle(
            Integer.parseInt(storeOptions.getOrDefault("max-idle", DEFAULT_REDIS_MAX_IDLE)));
        jedisPoolConfig.setMaxWaitMillis(
            Integer.parseInt(
                storeOptions.getOrDefault("max-wait-millis", DEFAULT_REDIS_MAX_WAIT_MILLIS)));
        JedisPool jedisPool =
            new JedisPool(
                jedisPoolConfig,
                storeOptions.get("host"),
                Integer.parseInt(storeOptions.get("port")));
        return new RedisBackedJobService(jedisPool);
>>>>>>> 901b7b0... Pr Comments and using ttl for cassandra job service
      case INVALID:
      case BIGQUERY:
      case CASSANDRA:
        FeastProperties.StoreProperties storeProperties = feastProperties.getStore();
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(
            HostDistance.LOCAL, storeProperties.getCassandraPoolCoreLocalConnections());
        poolingOptions.setCoreConnectionsPerHost(
            HostDistance.REMOTE, storeProperties.getCassandraPoolCoreRemoteConnections());
        poolingOptions.setMaxConnectionsPerHost(
            HostDistance.LOCAL, storeProperties.getCassandraPoolMaxLocalConnections());
        poolingOptions.setMaxConnectionsPerHost(
            HostDistance.REMOTE, storeProperties.getCassandraPoolMaxRemoteConnections());
        poolingOptions.setMaxRequestsPerConnection(
            HostDistance.LOCAL, storeProperties.getCassandraPoolMaxRequestsLocalConnection());
        poolingOptions.setMaxRequestsPerConnection(
            HostDistance.REMOTE, storeProperties.getCassandraPoolMaxRequestsRemoteConnection());
        poolingOptions.setNewConnectionThreshold(
            HostDistance.LOCAL, storeProperties.getCassandraPoolNewLocalConnectionThreshold());
        poolingOptions.setNewConnectionThreshold(
            HostDistance.REMOTE, storeProperties.getCassandraPoolNewRemoteConnectionThreshold());
        poolingOptions.setPoolTimeoutMillis(storeProperties.getCassandraPoolTimeoutMillis());
        StoreProto.Store.CassandraConfig cassandraConfig = store.getCassandraConfig();
        List<InetSocketAddress> contactPoints =
            Arrays.stream(storeOptions.get("bootstrapHosts").split(","))
                .map(h -> new InetSocketAddress(h, Integer.parseInt(storeOptions.get("port"))))
                .collect(Collectors.toList());
        Cluster cluster =
            Cluster.builder()
                .addContactPointsWithPorts(contactPoints)
                .withPoolingOptions(poolingOptions)
                .build();
        // Session in Cassandra is thread-safe and maintains connections to cluster nodes internally
        // Recommended to use one session per keyspace instead of open and close connection for each
        // request
        Session session;

        try {
          String keyspace = storeOptions.get("keyspace");
          KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
          if (keyspaceMetadata == null) {
            log.info("Creating keyspace '{}'", keyspace);
            Map<String, Object> replicationOptions = new HashMap<>();
            replicationOptions.put("class", storeOptions.get("replicationOptionsClass"));
            replicationOptions.put("stage-us-west1", storeOptions.get("replicationOptionsWest"));
            KeyspaceOptions createKeyspace =
                SchemaBuilder.createKeyspace(keyspace)
                    .ifNotExists()
                    .with()
                    .replication(replicationOptions);
            session = cluster.newSession();
            session.execute(createKeyspace);
          }

          session = cluster.connect(keyspace);
          // Currently no support for creating table from entity mapper:
          // https://datastax-oss.atlassian.net/browse/JAVA-569
          Create createTable =
              SchemaBuilder.createTable(keyspace, storeOptions.get("tableName"))
                  .ifNotExists()
                  .addPartitionKey("job_uuid", DataType.text())
                  .addClusteringColumn("timestamp", DataType.timestamp())
                  .addColumn("job_data", DataType.text());
          log.info("Create Cassandra table if not exists..");
          session.execute(createTable);

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
        return new CassandraBackedJobService(session);
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported store type '%s' for job store", storeType));
    }
  }
}
