package feast.ingestion.util;

import com.google.cloud.bigquery.*;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.StorageSpecProto.StorageSpec;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static feast.specs.FeatureSpecProto.FeatureSpec;
import static feast.types.ValueProto.ValueType;

@Slf4j
public class SchemaUtil {
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

  private static TableDefinition createTableDefinition(Iterable<FeatureSpec> featureSpecs) {
    List<Field> fields = new ArrayList<>();
    log.debug("Table will have the following fields:");
    for (FeatureSpec featureSpec : featureSpecs) {
      Field field =
          Field.newBuilder(
                  featureSpec.getName(),
                  VALUE_TYPE_TO_STANDARD_SQL_TYPE.get(featureSpec.getValueType()))
              .setDescription(featureSpec.getDescription())
              .build();
      log.debug("- {}", field.toString());
      fields.add(field);
    }
    return StandardTableDefinition.of(Schema.of(fields));
  }

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
    TableDefinition tableDefinition = createTableDefinition(featureSpecs);
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
}
