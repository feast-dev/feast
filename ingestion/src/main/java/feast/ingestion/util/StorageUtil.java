package feast.ingestion.util;

import static feast.specs.FeatureSpecProto.FeatureSpec;
import static feast.types.ValueProto.ValueType;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.common.collect.ImmutableMap;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.tuple.Pair;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

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
@Slf4j
public class StorageUtil {
  private static final Map<ValueType.Enum, StandardSQLTypeName> VALUE_TYPE_TO_STANDARD_SQL_TYPE =
      new HashMap<>();

  static {
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.BOOL, StandardSQLTypeName.BOOL);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.INT32, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.INT64, StandardSQLTypeName.INT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.BYTES, StandardSQLTypeName.BYTES);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.FLOAT, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.DOUBLE, StandardSQLTypeName.FLOAT64);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.TIMESTAMP, StandardSQLTypeName.TIMESTAMP);
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.STRING, StandardSQLTypeName.STRING);
  }

  private static TableDefinition createBigQueryTableDefinition(
      EntitySpec entitySpec, Iterable<FeatureSpec> featureSpecs) {
    List<Field> fields = new ArrayList<>();
    log.debug("Table will have the following fields:");
    for (FeatureSpec featureSpec : featureSpecs) {
      if (!entitySpec.getName().equals(featureSpec.getEntity())) {
        throw new IllegalArgumentException(
            String.format(
                "Entity specified in feature id '%s' is '%s', different from entity specified in entity spec '%s'. Please make sure they are the same and retry.",
                featureSpec.getId(), featureSpec.getEntity(), entitySpec.getName()));
      }

      Field field =
          Field.newBuilder(
                  featureSpec.getName(),
                  VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(featureSpec.getValueType()))
              .setDescription(featureSpec.getDescription())
              .build();
      log.debug("- {}", field.toString());
      fields.add(field);
    }

    // In Feast 0.2,
    // - "id" is a reserved field in BigQuery that indicates the entity id
    // - "event_timestamp" is a reserved field in BigQuery that indicates the event time for a
    // FeatureRow
    // - "created_timestamp" is a reserved field in BigQuery that indicates the time a FeatureRow is
    // crated in Feast
    // - "job_id" is a reserved field in BigQuery that indicates the Feast import job id that writes
    // the FeatureRows
    Map<String, Pair<StandardSQLTypeName, String>>
        reservedFieldNameToPairOfStandardSQLTypeAndDescription =
            ImmutableMap.of(
                "id", Pair.of(StandardSQLTypeName.STRING, "Entity ID for the FeatureRow"),
                "event_timestamp",
                    Pair.of(StandardSQLTypeName.TIMESTAMP, "Event time for the FeatureRow"),
                "created_timestamp",
                    Pair.of(
                        StandardSQLTypeName.TIMESTAMP,
                        "The time when the FeatureRow is created in Feast"),
                "job_id",
                    Pair.of(StandardSQLTypeName.STRING, "Feast import job ID for the FeatureRow"));
    for (Map.Entry<String, Pair<StandardSQLTypeName, String>> entry :
        reservedFieldNameToPairOfStandardSQLTypeAndDescription.entrySet()) {
      Field field =
          Field.newBuilder(entry.getKey(), entry.getValue().getLeft())
              .setDescription(entry.getValue().getRight())
              .build();
      log.debug("- {}", field.toString());
      fields.add(field);
    }

    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(Type.DAY).setField("event_timestamp").build();

    return StandardTableDefinition.newBuilder()
        .setTimePartitioning(timePartitioning)
        .setSchema(Schema.of(fields))
        .build();
  }

  /**
   * Setup BigQuery to ensure the dataset and table required to store features for the entity are
   * created or updated.
   *
   * @param storageSpec <a
   *     href="https://github.com/gojek/feast/blob/master/protos/feast/specs/StorageSpec.proto">StorageSpec</a>
   * @param entitySpec <a
   *     href="https://github.com/gojek/feast/blob/master/protos/feast/specs/EntitySpec.proto">EntitySpec</a>
   * @param featureSpecs List of <a
   *     href="https://github.com/gojek/feast/blob/master/protos/feast/specs/FeatureSpec.proto">FeatureSpecs</a>
   * @param bigquery <a
   *     href="https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/BigQuery.html">BigQuery</a>
   *     client service
   */
  public static void setupBigQuery(
      StorageSpec storageSpec,
      EntitySpec entitySpec,
      Iterable<FeatureSpec> featureSpecs,
      BigQuery bigquery) {
    String projectId =
        storageSpec.getOptionsOrDefault("projectId", bigquery.getOptions().getProjectId());
    assert projectId != null;

    // Ensure BigQuery dataset exists.
    DatasetId datasetId = DatasetId.of(projectId, storageSpec.getOptionsOrThrow("datasetId"));
    if (bigquery.getDataset(datasetId) == null) {
      log.info("Creating dataset '{}' in project '{}'", datasetId.getDataset(), projectId);
      bigquery.create(DatasetInfo.of(datasetId));
    }

    // Ensure BigQuery table with correct schema exists.
    TableId tableId = TableId.of(projectId, datasetId.getDataset(), entitySpec.getName());
    TableDefinition tableDefinition = createBigQueryTableDefinition(entitySpec, featureSpecs);
    TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);
    if (bigquery.getTable(tableId) == null) {
      log.info(
          "Creating table '{}' in dataset '{}' in project '{}'",
          tableId.getTable(),
          datasetId.getDataset(),
          projectId);
      bigquery.create(tableInfo);
    } else {
      log.info(
          "Updating table '{}' in dataset '{}' in project '{}'",
          tableId.getTable(),
          datasetId.getDataset(),
          projectId);
      bigquery.update(tableInfo);
    }
  }

  public static void checkRedisConnection(StorageSpec sinkStorageSpec) {
    if (!sinkStorageSpec.getOptions().containsKey("host")) {
      throw new IllegalArgumentException(
          "sinkStorageSpec type 'REDIS' requires 'host' options");
    }
    String redisHost = sinkStorageSpec.getOptionsOrThrow("host");
    int redisPort = Integer.parseInt(sinkStorageSpec.getOptionsOrDefault("port", "6379"));
    boolean clusterEnabled =
        Boolean.parseBoolean(sinkStorageSpec.getOptionsOrDefault("clusterEnabled", "false"));
    if (clusterEnabled) {
      throw new IllegalArgumentException(
          "Redis cluster is not supported in Feast 0.2. Please set 'clusterEnabled: false' in sinkStorageSpec.options.");
    }
    JedisPool jedisPool = new JedisPool(sinkStorageSpec.getOptionsOrThrow("host"), redisPort);
    try {
      // Make sure we can connect to Redis according to the sinkStorageSpec
      jedisPool.getResource();
    } catch (JedisConnectionException e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to Redis at host: '%s' port: '%d'. Please check the 'options' values in sinkStorageSpec.",
              redisHost, redisPort));
    }
    jedisPool.close();
  }
}
