/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.storage;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.common.base.Strings;
import com.google.protobuf.util.JsonFormat;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import feast.types.ValueProto.ValueType.Enum;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class BigQueryStorageManager implements StorageManager {

  public static final String TYPE = "bigquery";

  public static final String OPT_BIGQUERY_PROJECT = "project";
  public static final String OPT_BIGQUERY_DATASET = "dataset";
  public static final String OPT_BIGQUERY_TEMP_LOCATION = "tempLocation"; // gcs or local.

  private static final String FIELD_ID = "id";
  private static final String FIELD_EVENT_TIMESTAMP = "event_timestamp";
  private static final String FIELD_CREATED_TIMESTAMP = "created_timestamp";
  private static final String FIELD_JOB_ID = "job_id";

  private String id;
  private final BigQuery bigQuery;
  private final String datasetName;
  private final String project;
  private final BigQueryViewTemplater viewTemplater;

  /**
   * Mapping of Bigquery types to feast supported types
   */
  private static final Map<ValueType.Enum, LegacySQLTypeName> FEAST_TO_BIGQUERY_TYPE_MAP =
      new HashMap<>();

  static {
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.BOOL, LegacySQLTypeName.BOOLEAN);
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.INT32, LegacySQLTypeName.INTEGER);
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.INT64, LegacySQLTypeName.INTEGER);
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.BYTES, LegacySQLTypeName.BYTES);
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.FLOAT, LegacySQLTypeName.FLOAT);
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.DOUBLE, LegacySQLTypeName.FLOAT);
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.TIMESTAMP, LegacySQLTypeName.TIMESTAMP);
    FEAST_TO_BIGQUERY_TYPE_MAP.put(Enum.STRING, LegacySQLTypeName.STRING);
  }

  public BigQueryStorageManager(
      String id,
      BigQuery bigQuery,
      String project,
      String datasetName,
      BigQueryViewTemplater viewTemplater) {
    this.id = id;
    this.bigQuery = bigQuery;
    this.datasetName = datasetName;
    this.project = project;
    this.viewTemplater = viewTemplater;
  }

  /**
   * Update the BigQuery schema of this table given the addition of this feature.
   *
   * @param featureSpec specification of the new feature.
   */
  @Override
  public void registerNewFeature(FeatureSpec featureSpec) {
    if (!isDatasetExists(datasetName)) {
      DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
      bigQuery.create(datasetInfo);
    }

    String tableName = createTableName(featureSpec);
    TableId tableId = TableId.of(datasetName, tableName);

    Table table = bigQuery.getTable(tableId);
    if (table == null) {
      Schema schema =
          Schema.of(
              createField(FIELD_ID, Enum.STRING, ""),
              createField(FIELD_EVENT_TIMESTAMP, Enum.TIMESTAMP, FIELD_EVENT_TIMESTAMP),
              createField(FIELD_CREATED_TIMESTAMP, Enum.TIMESTAMP, FIELD_CREATED_TIMESTAMP),
              createField(FIELD_JOB_ID, Enum.STRING, FIELD_JOB_ID),
              createFeatureField(featureSpec));
      TableDefinition tableDefinition =
          StandardTableDefinition.newBuilder()
              .setSchema(schema)
              .setTimePartitioning(TimePartitioning.of(Type.DAY))
              .build();
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
      checkTableCreation(bigQuery.create(tableInfo), featureSpec);

      createOrUpdateView(tableName, Arrays.asList(featureSpec.getName()));
      return;
    }

    Schema existingSchema = table.getDefinition().getSchema();
    Field newField = createFeatureField(featureSpec);
    if (isFieldExist(newField, existingSchema)) {
      return;
    }

    List<Field> fields = new ArrayList<>(existingSchema.getFields());
    fields.add(newField);
    Schema newSchema = Schema.of(fields);
    TableDefinition tableDefinition = StandardTableDefinition.of(newSchema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
    checkTableCreation(bigQuery.update(tableInfo), featureSpec);
    createOrUpdateView(
        tableName,
        fields
            .stream()
            .map(Field::getName)
            .filter(
                f ->
                    !f.equals(FIELD_ID)
                        && !f.equals(FIELD_CREATED_TIMESTAMP)
                        && !f.equals(FIELD_EVENT_TIMESTAMP)
                        && !f.equals(FIELD_JOB_ID))

            .collect(Collectors.toList()));
    AuditLogger.log(
        Resource.STORAGE,
        this.id,
        Action.SCHEMA_UPDATE,
        "Bigquery schema updated for feature %s",
        featureSpec.getId());
  }

  private void checkTableCreation(Table table, FeatureSpec featureSpec) {
    if (table == null) {
      throw new StorageInitializationException(
          Strings.lenientFormat(
              "Bigquery table creation failed. Possibly linked to BQ rate limiting, please try again later."));
    } else {
      FieldList fields = table.getDefinition().getSchema().getFields();
      List<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toList());
      if (!fieldNames.contains(featureSpec.getName())) {
        throw new StorageInitializationException(
            Strings.lenientFormat(
                "Bigquery table creation failed. Possibly linked to BQ rate limiting, please try again later."));
      }
    }
  }

  private boolean isFieldExist(Field field, Schema existingSchema) {
    for (Field existingField : existingSchema.getFields()) {
      if (field.getName().equals(existingField.getName())) {
        return true;
      }
    }
    return false;
  }

  private Field createField(String name, Enum valueType, String description) {
    Field.Builder fieldBuilder = Field.newBuilder(name, FEAST_TO_BIGQUERY_TYPE_MAP.get(valueType));
    if (description != null) {
      fieldBuilder.setDescription(description);
    }
    return fieldBuilder.build();
  }

  private Field createFeatureField(FeatureSpec featureSpec) {
    return createField(
        featureSpec.getName(), featureSpec.getValueType(), featureSpec.getDescription());
  }

  private String createTableName(FeatureSpec featureSpec) {
    String entityName = featureSpec.getEntity().toLowerCase();
    return String.format("%s", entityName);
  }

  private void createOrUpdateView(String tableName, List<String> features) {
    String query = viewTemplater.getViewQuery(project, datasetName, tableName, features);
    String viewName = String.join("_", tableName, "view");
    if (isViewExists(datasetName, viewName)) {
      bigQuery.update(TableInfo.of(TableId.of(datasetName, viewName), ViewDefinition.of(query)));
      return;
    }
    bigQuery.create(TableInfo.of(TableId.of(datasetName, viewName), ViewDefinition.of(query)));
  }

  private boolean isViewExists(String datasetName, String viewName) {
    return bigQuery.getTable(TableId.of(datasetName, viewName)) != null;
  }

  private boolean isDatasetExists(String datasetName) {
    return bigQuery.getDataset(datasetName) != null;
  }
}
