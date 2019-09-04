package feast.ingestion.util;

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
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store.RedisConfig;
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
    VALUE_TYPE_TO_STANDARD_SQL_TYPE.put(ValueType.Enum.STRING, StandardSQLTypeName.STRING);
  }

  private static TableDefinition createBigQueryTableDefinition(FeatureSetSpec featureSetSpec) {
    List<Field> fields = new ArrayList<>();

    log.info("Table will have the following fields:");
    for (EntitySpec entitySpec : featureSetSpec.getEntitiesList()) {
      Field field =
          Field.newBuilder(
                  entitySpec.getName(),
                  VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(entitySpec.getValueType()))
              .build();
      log.info("- {}", field.toString());
      fields.add(field);
    }
    for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
      Field field =
          Field.newBuilder(
                  featureSpec.getName(),
                  VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(featureSpec.getValueType()))
              .build();
      log.info("- {}", field.toString());
      fields.add(field);
    }

    // In Feast 0.3
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
                "id",
                Pair.of(StandardSQLTypeName.STRING, "Entity ID for the FeatureRow"),
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
      log.info("- {}", field.toString());
      fields.add(field);
    }

    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(Type.DAY).setField("event_timestamp").build();

    return StandardTableDefinition.newBuilder()
        .setTimePartitioning(timePartitioning)
        .setSchema(Schema.of(fields))
        .build();
  }

  /** TODO: Add documentation */
  public static void setupBigQuery(
      FeatureSetSpec featureSetSpec,
      String bigqueryProjectId,
      String bigqueryDatasetId,
      BigQuery bigquery) {
    // Ensure BigQuery dataset exists.
    DatasetId datasetId = DatasetId.of(bigqueryProjectId, bigqueryDatasetId);
    if (bigquery.getDataset(datasetId) == null) {
      log.info("Creating dataset '{}' in project '{}'", datasetId.getDataset(), bigqueryProjectId);
      bigquery.create(DatasetInfo.of(datasetId));
    }

    // Ensure BigQuery table with correct schema exists.
    String tableName =
        String.format("%s_%d", featureSetSpec.getName(), featureSetSpec.getVersion());
    TableId tableId = TableId.of(bigqueryProjectId, datasetId.getDataset(), tableName);
    TableDefinition tableDefinition = createBigQueryTableDefinition(featureSetSpec);
    TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);
    if (bigquery.getTable(tableId) == null) {
      log.info(
          "Creating table '{}' in dataset '{}' in project '{}'",
          tableId.getTable(),
          datasetId.getDataset(),
          bigqueryProjectId);
      bigquery.create(tableInfo);
    } else {
      log.info(
          "Updating table '{}' in dataset '{}' in project '{}'",
          tableId.getTable(),
          datasetId.getDataset(),
          bigqueryProjectId);
      bigquery.update(tableInfo);
    }
  }

  /**
   * Ensure Redis is accessible, else throw a RuntimeException.
   *
   * @param redisConfig Plase refer to feast.core.Store proto
   */
  public static void checkRedisConnection(RedisConfig redisConfig) {
    JedisPool jedisPool = new JedisPool(redisConfig.getHost(), redisConfig.getPort());
    try {
      jedisPool.getResource();
    } catch (JedisConnectionException e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to Redis at host: '%s' port: '%d'. Please check that your Redis is running and accessible from Feast.",
              redisConfig.getHost(), redisConfig.getPort()));
    }
    jedisPool.close();
  }
}
