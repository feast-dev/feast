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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class BigQueryStorageManagerTest {

  private BigQueryStorageManager bqManager;
  @Mock
  private BigQuery bigQuery;
  private String datasetName;
  private String projectId;
  private BigQueryViewTemplater templater;

  @Before
  public void setUp() throws Exception {
    datasetName = "dummyDataset";

    MockitoAnnotations.initMocks(this);
    BigQueryViewTemplater templater =
        new BigQueryViewTemplater("{{tableName}}{{#features}}.{{name}}{{/features}}");
    bqManager = new BigQueryStorageManager("BIGQUERY1", bigQuery, projectId, datasetName, templater);
  }

  @Test
  public void shouldCreateNewDatasetAndTableAndViewIfNotExist() throws InterruptedException {
    when(bigQuery.getDataset(any(String.class))).thenReturn(null);
    String featureName = "my_feature";
    String entityName = "my_entity";
    String featureId = createFeatureId(entityName, featureName);
    String description = "my feature is awesome";
    ValueType.Enum type = ValueType.Enum.INT64;

    FeatureSpec fs =
        FeatureSpec.newBuilder()
            .setEntity(entityName)
            .setId(featureId)
            .setName(featureName)
            .setDescription(description)
            .setValueType(type)
            .build();

    ArgumentCaptor<DatasetInfo> datasetArg = ArgumentCaptor.forClass(DatasetInfo.class);
    ArgumentCaptor<TableInfo> tableInfoArg = ArgumentCaptor.forClass(TableInfo.class);

    Table expectedTable = mock(Table.class);
    TableDefinition definition = StandardTableDefinition
        .of(Schema.of(Field.of(featureName, LegacySQLTypeName.INTEGER)));
    when(expectedTable.getDefinition()).thenReturn(definition);
    when(bigQuery.create(any(TableInfo.class))).thenReturn(expectedTable);

    bqManager.registerNewFeature(fs);

    verify(bigQuery).create(datasetArg.capture());
    assertThat(datasetArg.getValue().getDatasetId().getDataset(), equalTo(datasetName));

    verify(bigQuery, times(2)).create(tableInfoArg.capture());
    List<TableInfo> capturedValues = tableInfoArg.getAllValues();

    // verify table is created
    TableInfo actualTable = capturedValues.get(0);
    assertThat(
        actualTable.getTableId().getTable(),
        equalTo(String.format("%s", entityName)));
    List<Field> fields = actualTable.getDefinition().getSchema().getFields();
    assertThat(fields.size(), equalTo(5));
    Field idField = fields.get(0);
    assertThat(idField.getName(), equalTo("id"));
    Field etsField = fields.get(1);
    assertThat(etsField.getName(), equalTo("event_timestamp"));
    Field ctsField = fields.get(2);
    assertThat(ctsField.getName(), equalTo("created_timestamp"));
    Field field = fields.get(4);
    assertThat(field.getDescription(), equalTo(description));
    Field jobIdField = fields.get(3);
    assertThat(jobIdField.getName(), equalTo("job_id"));
    assertThat(field.getType(), equalTo(LegacySQLTypeName.INTEGER));
    assertThat(field.getName(), equalTo(featureName));

    // verify view is created
    TableInfo actualView = capturedValues.get(1);
    assertThat(
        actualView.getTableId().getTable(),
        equalTo(String.format("%s_view", entityName)));
    ViewDefinition actualDefinition = actualView.getDefinition();
    assertThat(
        actualDefinition.getQuery(),
        equalTo(
            String.format(
                "%s.%s", entityName, featureName)));
  }

  @Test
  public void shouldNotUpdateTableIfColumnExists() {
    String featureName = "my_feature";
    String entityName = "my_entity";
    String featureId = createFeatureId(entityName, featureName);
    String description = "my feature is awesome";
    ValueType.Enum type = ValueType.Enum.BOOL;
    LegacySQLTypeName sqlType = LegacySQLTypeName.BOOLEAN;

    Table table = mock(Table.class);
    TableDefinition tableDefinition = mock(TableDefinition.class);
    when(table.getDefinition()).thenReturn(tableDefinition);

    Schema schema = mock(Schema.class);
    when(tableDefinition.getSchema()).thenReturn(schema);
    Field field = Field.of(featureName, sqlType);
    FieldList fields = FieldList.of(field);
    when(schema.getFields()).thenReturn(fields);

    when(bigQuery.getDataset(any(String.class))).thenReturn(mock(Dataset.class));
    when(bigQuery.getTable(any(TableId.class))).thenReturn(table);
    FeatureSpec fs =
        FeatureSpec.newBuilder()
            .setEntity(entityName)
            .setId(featureId)
            .setName(featureName)
            .setDescription(description)
            .setValueType(type)
            .build();

    bqManager.registerNewFeature(fs);

    verify(bigQuery, never()).update(any(TableInfo.class));
    verify(bigQuery, never()).create(any(TableInfo.class));
  }

  @Test
  public void shouldUpdateTableAndViewIfColumnNotExists() {
    String newFeatureName = "my_feature";
    String entityName = "my_entity";
    String featureId = createFeatureId(entityName, newFeatureName);
    String description = "my feature is awesome";
    ValueType.Enum type = ValueType.Enum.BOOL;
    LegacySQLTypeName sqlType = LegacySQLTypeName.BOOLEAN;
    String existingFeatureName = "old_feature";

    FeatureSchema oldFeatureSchema = new FeatureSchema(existingFeatureName, sqlType, description);
    FeatureSchema newFeatureSchema = new FeatureSchema(newFeatureName, sqlType, description);
    Table oldTable = createTable(oldFeatureSchema);
    Table newTable = createTable(oldFeatureSchema, newFeatureSchema);

    when(bigQuery.getDataset(any(String.class))).thenReturn(mock(Dataset.class));
    when(bigQuery.getTable(any(TableId.class))).thenReturn(oldTable);
    FeatureSpec fs =
        FeatureSpec.newBuilder()
            .setEntity(entityName)
            .setId(featureId)
            .setName(newFeatureName)
            .setDescription(description)
            .setValueType(type)
            .build();

    when(bigQuery.update(any(TableInfo.class))).thenReturn(newTable);

    bqManager.registerNewFeature(fs);

    ArgumentCaptor<TableInfo> updateTableArg = ArgumentCaptor.forClass(TableInfo.class);
    verify(bigQuery, times(2)).update(updateTableArg.capture());

    List<TableInfo> capturedArgs = updateTableArg.getAllValues();

    // check table
    TableInfo actualTable = capturedArgs.get(0);
    FieldList actualFieldList = actualTable.getDefinition().getSchema().getFields();
    assertThat(actualFieldList.size(), equalTo(2));
    assertThat(actualFieldList.get(0),
        equalTo(oldTable.getDefinition().getSchema().getFields().get(0)));
    Field newField = Field.newBuilder(newFeatureName, sqlType).setDescription(description).build();
    assertThat(actualFieldList.get(1), equalTo(newField));

    // check view
    TableInfo actualView = capturedArgs.get(1);
    assertThat(
        actualView.getTableId().getTable(),
        equalTo(String.format("%s_view", entityName)));
    ViewDefinition actualDefinition = actualView.getDefinition();
    assertThat(
        actualDefinition.getQuery(),
        equalTo(
            String.format(
                "%s.%s.%s",
                entityName,
                existingFeatureName,
                newFeatureName)));
  }

  private String createFeatureId(
      String entityName, String featureName) {
    return String.format("%s.%s", entityName, featureName).toLowerCase();
  }

  private Table createTable(FeatureSchema... featureSchemas) {
    Table table = mock(Table.class);
    TableDefinition tableDefinition = mock(TableDefinition.class);
    when(table.getDefinition()).thenReturn(tableDefinition);

    Schema schema = mock(Schema.class);
    when(tableDefinition.getSchema()).thenReturn(schema);

    List<Field> fieldList = new ArrayList<>();
    for (FeatureSchema featureSchema : featureSchemas) {
      fieldList.add(
          Field.newBuilder(featureSchema.getFeatureName(), featureSchema.getType())
              .setDescription(featureSchema.getDescription())
              .build());
    }
    FieldList fields = FieldList.of(fieldList);
    when(schema.getFields()).thenReturn(fields);
    return table;
  }

  @AllArgsConstructor
  @Getter
  private static class FeatureSchema {
    String featureName;
    LegacySQLTypeName type;
    String description;
  }
}
