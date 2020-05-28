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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.CassandraConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.FeastProperties;
import feast.serving.FeastProperties.StoreProperties;
import feast.serving.service.BigQueryServingService;
import feast.serving.service.CassandraServingService;
import feast.serving.service.JobService;
import feast.serving.service.NoopJobService;
import feast.serving.service.RedisServingService;
import feast.serving.service.ServingService;
import feast.serving.specs.CachedSpecService;
import io.opentracing.Tracer;
import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
        CassandraConfig cassandraConfig =
            CassandraConfig.newBuilder()
                .setBootstrapHosts(options.get("host"))
                .setPort(Integer.parseInt(options.get("port")))
                .setKeyspace(options.get("keyspace"))
                .build();
        return builder.setCassandraConfig(cassandraConfig).build();
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
        servingService =
            new RedisServingService(
                storeConfiguration.getServingRedisConnection(), specService, tracer);
        break;
      case BIGQUERY:
        BigQueryConfig bqConfig = store.getBigqueryConfig();
        GoogleCredentials credentials;
        File credentialsPath =
            new File(
                "/etc/gcloud/service-accounts/credentials.json"); // TODO: update to your key path.
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
          credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (Exception e) {
          throw new IllegalStateException("No credentials file found", e);
        }
        BigQuery bigquery =
            BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(feastProperties.getJobs().getStagingProject())
                .build()
                .getService();
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
                feastProperties.getJobs().getBigqueryInitialRetryDelaySecs(),
                feastProperties.getJobs().getBigqueryTotalTimeoutSecs(),
                storage);
        break;
      case CASSANDRA:
        StoreProperties storeProperties = feastProperties.getStore();
        CassandraConfig cassandraConfig = store.getCassandraConfig();
        List<InetSocketAddress> contactPoints =
            Arrays.stream(cassandraConfig.getBootstrapHosts().split(","))
                .map(h -> new InetSocketAddress(h, cassandraConfig.getPort()))
                .collect(Collectors.toList());
        CqlSession cluster =
            CqlSession.builder()
                .addContactPoints(contactPoints)
                .withLocalDatacenter(storeProperties.getCassandraDcName())
                .build();
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
        ConsistencyLevel cl;
        log.info(String.format("Consistency level: %s", cassandraConfig.getConsistency()));
        if (cassandraConfig.getConsistency().equals("ONE")) {
          log.info("Serving with read consistency ONE");
          cl = ConsistencyLevel.ONE;
        } else {
          log.info("Serving with read consistency TWO");
          cl = ConsistencyLevel.TWO;
        }
        servingService =
            new CassandraServingService(
                cluster,
                cassandraConfig.getKeyspace(),
                cassandraConfig.getTableName(),
                cassandraConfig.getVersionless(),
                cl,
                specService,
                tracer);
        break;
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
