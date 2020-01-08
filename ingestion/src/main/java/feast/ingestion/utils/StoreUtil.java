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
package feast.ingestion.utils;

import static feast.types.ValueProto.ValueType;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Builder;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.common.collect.ImmutableMap;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.CassandraConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.store.serving.cassandra.CassandraMutation;
import feast.types.ValueProto.ValueType.Enum;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

// TODO: Create partitioned table by default

/**
 * This class is a utility to manage storage backends in Feast.
 *
 * <p>Examples when schemas need to be updated:
 *
 * <ul>
 *   <li>when a new entity is registered, a table usually needs to be created
 *   <li>when a new feature is registered, a column with appropriate data type usually needs to be
 *       created
 * </ul>
 *
 * <p>If the storage backend is a key-value or a schema-less database, however, there may not be a
 * need to manage any schemas. This class will not be used in that case.
 */
public class StoreUtil {

  private static final Map<ValueType.Enum, StandardSQLTypeName> VALUE_TYPE_TO_STANDARD_SQL_TYPE =
      new HashMap<>();
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(StoreUtil.class);

  // Column description for reserved fields
  public static final String BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION =
      "Event time for the FeatureRow";
  public static final String BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION =
      "Processing time of the FeatureRow ingestion in Feast\"";
  public static final String BIGQUERY_JOB_ID_FIELD_DESCRIPTION =
      "Feast import job ID for the FeatureRow";

  // Refer to protos/feast/core/Store.proto for the mapping definition.
  static {
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.BYTES, StandardSQLTypeName.BYTES);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.STRING, StandardSQLTypeName.STRING);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.INT32, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.INT64, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.DOUBLE, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.FLOAT, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.BOOL, StandardSQLTypeName.BOOL);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.BYTES_LIST, StandardSQLTypeName.BYTES);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.STRING_LIST, StandardSQLTypeName.STRING);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.INT32_LIST, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.INT64_LIST, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.DOUBLE_LIST, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.FLOAT_LIST, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(Enum.BOOL_LIST, StandardSQLTypeName.BOOL);
  }

  public static void setupStore(Store store, FeatureSet featureSet) {
    StoreType storeType = store.getType();
    switch (storeType) {
      case REDIS:
        StoreUtil.checkRedisConnection(store.getRedisConfig());
        break;
      case BIGQUERY:
        StoreUtil.setupBigQuery(
            featureSet,
            store.getBigqueryConfig().getProjectId(),
            store.getBigqueryConfig().getDatasetId(),
            BigQueryOptions.getDefaultInstance().getService());
        break;
      case CASSANDRA:
        StoreUtil.setupCassandra(store.getCassandraConfig());
        break;
      default:
        log.warn("Store type '{}' is unsupported", storeType);
        break;
    }
  }

  @SuppressWarnings("DuplicatedCode")
  public static TableDefinition createBigQueryTableDefinition(FeatureSetSpec featureSetSpec) {
    List<Field> fields = new ArrayList<>();
    log.info("Table will have the following fields:");

    for (EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      Builder builder =
          Field.newBuilder(
              entitySpec.getName(), VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(entitySpec.getValueType()));
      if (entitySpec.getValueType().name().toLowerCase().endsWith("_list")) {
        builder.setMode(Mode.REPEATED);
      }
      Field field = builder.build();
      log.info("- {}", field.toString());
      fields.add(field);
    }
    for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
      Builder builder =
          Field.newBuilder(
              featureSpec.getName(),
              VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(featureSpec.getValueType()));
      if (featureSpec.getValueType().name().toLowerCase().endsWith("_list")) {
        builder.setMode(Mode.REPEATED);
      }
      Field field = builder.build();
      log.info("- {}", field.toString());
      fields.add(field);
    }

    // Refer to protos/feast/core/Store.proto for reserved fields in BigQuery.
    Map<String, Pair<StandardSQLTypeName, String>>
        reservedFieldNameToPairOfStandardSQLTypeAndDescription =
            ImmutableMap.of(
                "event_timestamp",
                Pair.of(StandardSQLTypeName.TIMESTAMP, BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION),
                "created_timestamp",
                Pair.of(
                    StandardSQLTypeName.TIMESTAMP, BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION),
                "job_id",
                Pair.of(StandardSQLTypeName.STRING, BIGQUERY_JOB_ID_FIELD_DESCRIPTION));
    for (Map.Entry<String, Pair<StandardSQLTypeName, String>> entry :
        reservedFieldNameToPairOfStandardSQLTypeAndDescription.entrySet()) {
      Field field =
          Field.newBuilder(entry.getKey(), entry.getValue().getLeft())
              .setDescription(entry.getValue().getRight())
              .build();
      log.info("- {}", field.toString());
      fields.add(field);
    }

    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(Type.DAY).setField("event_timestamp").build();
    log.info("Table partitioning: " + timePartitioning.toString());

    return StandardTableDefinition.newBuilder()
        .setTimePartitioning(timePartitioning)
        .setSchema(Schema.of(fields))
        .build();
  }

  /**
   * This method ensures that, given a FeatureSetSpec object, the relevant BigQuery table is created
   * with the correct schema.
   *
   * <p>Refer to protos/feast/core/Store.proto for the derivation of the table name and schema from
   * a FeatureSetSpec object.
   *
   * @param featureSet FeatureSet object
   * @param bigqueryProjectId BigQuery project id
   * @param bigqueryDatasetId BigQuery dataset id
   * @param bigquery BigQuery service object
   */
  public static void setupBigQuery(
      FeatureSet featureSet,
      String bigqueryProjectId,
      String bigqueryDatasetId,
      BigQuery bigquery) {

    FeatureSetSpec featureSetSpec = featureSet.getSpec();
    // Ensure BigQuery dataset exists.
    DatasetId datasetId = DatasetId.of(bigqueryProjectId, bigqueryDatasetId);
    if (bigquery.getDataset(datasetId) == null) {
      log.info("Creating dataset '{}' in project '{}'", datasetId.getDataset(), bigqueryProjectId);
      bigquery.create(DatasetInfo.of(datasetId));
    }

    String tableName =
        String.format(
                "%s_%s_v%d",
                featureSetSpec.getProject(), featureSetSpec.getName(), featureSetSpec.getVersion())
            .replaceAll("-", "_");
    TableId tableId = TableId.of(bigqueryProjectId, datasetId.getDataset(), tableName);

    // Return if there is an existing table
    Table table = bigquery.getTable(tableId);
    if (table != null) {
      log.info(
          "Writing to existing BigQuery table '{}:{}.{}'",
          bigqueryProjectId,
          datasetId.getDataset(),
          tableName);
      return;
    }

    log.info(
        "Creating table '{}' in dataset '{}' in project '{}'",
        tableId.getTable(),
        datasetId.getDataset(),
        bigqueryProjectId);
    TableDefinition tableDefinition = createBigQueryTableDefinition(featureSet.getSpec());
    TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);
    bigquery.create(tableInfo);
  }

  /**
   * Ensure Redis is accessible, else throw a RuntimeException.
   *
   * @param redisConfig Plase refer to feast.core.Store proto
   */
  public static void checkRedisConnection(RedisConfig redisConfig) {
    RedisClient redisClient =
        RedisClient.create(RedisURI.create(redisConfig.getHost(), redisConfig.getPort()));
    try {
      redisClient.connect();
    } catch (RedisConnectionException e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to Redis at host: '%s' port: '%d'. Please check that your Redis is running and accessible from Feast.",
              redisConfig.getHost(), redisConfig.getPort()));
    }
    redisClient.shutdown();
  }

  /**
   * Ensures Cassandra is accessible, else throw a RuntimeException. Creates Cassandra keyspace and
   * table if it does not already exist
   *
   * @param cassandraConfig Please refer to feast.core.Store proto
   */
  public static void setupCassandra(CassandraConfig cassandraConfig) {
    List<InetSocketAddress> contactPoints =
        Arrays.stream(cassandraConfig.getBootstrapHosts().split(","))
            .map(host -> new InetSocketAddress(host, cassandraConfig.getPort()))
            .collect(Collectors.toList());
    Cluster cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();
    Session session;

    try {
      String keyspace = cassandraConfig.getKeyspace();
      KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
      if (keyspaceMetadata == null) {
        log.info("Creating keyspace '{}'", keyspace);
        Map<String, Object> replicationOptions =
            cassandraConfig.getReplicationOptionsMap().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
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
          SchemaBuilder.createTable(keyspace, cassandraConfig.getTableName())
              .ifNotExists()
              .addPartitionKey(CassandraMutation.ENTITIES, DataType.text())
              .addClusteringColumn(CassandraMutation.FEATURE, DataType.text())
              .addColumn(CassandraMutation.VALUE, DataType.blob());
      log.info("Create Cassandra table if not exists..");
      session.execute(createTable);

      validateCassandraTable(session);

      session.close();
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
    cluster.close();
  }

  private static void validateCassandraTable(Session session) {
    try {
      new MappingManager(session).mapper(CassandraMutation.class).getTableMetadata();
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Table created does not match the datastax object mapper: %s",
              CassandraMutation.class.getSimpleName()));
    }
  }
}
