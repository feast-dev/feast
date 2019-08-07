package feast.ingestion.util;

import com.google.cloud.bigquery.*;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static feast.specs.FeatureSpecProto.FeatureSpec;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaUtilTest {
  private StorageSpec storageSpec;
  private EntitySpec entitySpec;
  private Iterable<FeatureSpec> featureSpecs;
  private List<Field> expectedFields;

  @Before
  public void createDefaultArguments() {
    Map<String, String> storageSpecOptions = ImmutableMap.of("datasetId", "test_dataset");
    storageSpec = StorageSpec.newBuilder().putAllOptions(storageSpecOptions).build();
    entitySpec = EntitySpec.newBuilder().setName("test_entity").build();
    featureSpecs =
        Arrays.asList(
            FeatureSpec.newBuilder()
                .setName("test_feature_1")
                .setEntity(entitySpec.getName())
                .setValueType(ValueType.Enum.INT64)
                .build(),
            FeatureSpec.newBuilder()
                .setName("test_feature_2")
                .setEntity(entitySpec.getName())
                .setDescription("test_feature_2 description")
                .setValueType(ValueType.Enum.STRING)
                .build());
    List<Field> reservedFields =
        Arrays.asList(
            Field.newBuilder("id", StandardSQLTypeName.STRING)
                .setDescription("Entity ID for the FeatureRow")
                .build(),
            Field.newBuilder("event_timestamp", StandardSQLTypeName.TIMESTAMP)
                .setDescription("Event time for the FeatureRow")
                .build(),
            Field.newBuilder("created_timestamp", StandardSQLTypeName.TIMESTAMP)
                .setDescription("The time when the FeatureRow is created in Feast")
                .build(),
            Field.newBuilder("job_id", StandardSQLTypeName.STRING)
                .setDescription("Feast import job ID for the FeatureRow")
                .build());
    expectedFields = new ArrayList<>();
    expectedFields.add(
        Field.newBuilder("test_feature_1", StandardSQLTypeName.INT64).setDescription("").build());
    expectedFields.add(
        Field.newBuilder("test_feature_2", StandardSQLTypeName.STRING)
            .setDescription("test_feature_2 description")
            .build());
    expectedFields.addAll(reservedFields);
  }

  @Test
  public void setupBigQueryWithNonExistingDataset() {
    // Mock BigQuery service
    BigQuery mockBigquery = Mockito.mock(BigQuery.class);
    BigQueryOptions mockBigqueryOptions = Mockito.mock(BigQueryOptions.class);
    when(mockBigqueryOptions.getProjectId()).thenReturn("test_project");
    when(mockBigquery.getOptions()).thenReturn(mockBigqueryOptions);

    SchemaUtil.setupBigQuery(storageSpec, entitySpec, featureSpecs, mockBigquery);

    // Ensure BigQuery service is called with correct arguments
    verify(mockBigquery).create(eq(DatasetInfo.of(DatasetId.of("test_project", "test_dataset"))));
    verify(mockBigquery)
        .create(
            eq(
                TableInfo.of(
                    TableId.of("test_project", "test_dataset", "test_entity"),
                    StandardTableDefinition.of(Schema.of(expectedFields)))));
  }

  @Test
  public void setupBigQueryWithExistingDataset() {
    // Mock BigQuery service
    BigQuery mockBigquery = Mockito.mock(BigQuery.class);
    BigQueryOptions mockBigqueryOptions = Mockito.mock(BigQueryOptions.class);
    when(mockBigqueryOptions.getProjectId()).thenReturn("test_project");
    when(mockBigquery.getOptions()).thenReturn(mockBigqueryOptions);
    when(mockBigquery.getDataset(any(DatasetId.class))).thenReturn(Mockito.mock(Dataset.class));

    SchemaUtil.setupBigQuery(storageSpec, entitySpec, featureSpecs, mockBigquery);

    // Ensure BigQuery service is called with correct arguments
    verify(mockBigquery)
        .create(
            eq(
                TableInfo.of(
                    TableId.of("test_project", "test_dataset", "test_entity"),
                    StandardTableDefinition.of(Schema.of(expectedFields)))));
  }

  @Test
  public void setupBigQueryWithExistingTable() {
    // Mock BigQuery service
    BigQuery mockBigquery = Mockito.mock(BigQuery.class);
    BigQueryOptions mockBigqueryOptions = Mockito.mock(BigQueryOptions.class);
    when(mockBigqueryOptions.getProjectId()).thenReturn("test_project");
    when(mockBigquery.getOptions()).thenReturn(mockBigqueryOptions);
    when(mockBigquery.getDataset(any(DatasetId.class))).thenReturn(Mockito.mock(Dataset.class));
    when(mockBigquery.getTable(any(TableId.class))).thenReturn(Mockito.mock(Table.class));

    SchemaUtil.setupBigQuery(storageSpec, entitySpec, featureSpecs, mockBigquery);

    // Ensure BigQuery service is called with correct arguments
    verify(mockBigquery)
        .update(
            eq(
                TableInfo.of(
                    TableId.of("test_project", "test_dataset", "test_entity"),
                    StandardTableDefinition.of(Schema.of(expectedFields)))));
  }

  @Test
  public void setupBigQueryWithProjectIdInStorageSpec() {
    Map<String, String> storageSpecOptions =
        ImmutableMap.of("datasetId", "test_dataset", "projectId", "project_from_storage_spec");
    storageSpec = StorageSpec.newBuilder().putAllOptions(storageSpecOptions).build();

    // Mock BigQuery service
    BigQuery mockBigquery = Mockito.mock(BigQuery.class);
    BigQueryOptions mockBigqueryOptions = Mockito.mock(BigQueryOptions.class);
    when(mockBigqueryOptions.getProjectId()).thenReturn("test_project");
    when(mockBigquery.getOptions()).thenReturn(mockBigqueryOptions);

    SchemaUtil.setupBigQuery(storageSpec, entitySpec, featureSpecs, mockBigquery);

    // Ensure BigQuery service is called with correct arguments
    verify(mockBigquery)
        .create(eq(DatasetInfo.of(DatasetId.of("project_from_storage_spec", "test_dataset"))));
    verify(mockBigquery)
        .create(
            eq(
                TableInfo.of(
                    TableId.of("project_from_storage_spec", "test_dataset", "test_entity"),
                    StandardTableDefinition.of(Schema.of(expectedFields)))));
  }

  @Test(expected = NullPointerException.class)
  public void setupBigQueryWithMissingDatasetIdInStorageSpec() {
    storageSpec = StorageSpec.newBuilder().build();

    // Mock BigQuery service
    BigQuery mockBigquery = Mockito.mock(BigQuery.class);
    SchemaUtil.setupBigQuery(storageSpec, entitySpec, featureSpecs, mockBigquery);
  }

  @Test(expected = BigQueryException.class)
  public void setupBigQueryWithExceptionWhenUpdatingTable() {
    // Mock BigQuery service
    BigQuery mockBigquery = Mockito.mock(BigQuery.class);
    BigQueryOptions mockBigqueryOptions = Mockito.mock(BigQueryOptions.class);
    when(mockBigqueryOptions.getProjectId()).thenReturn("test_project");
    when(mockBigquery.getOptions()).thenReturn(mockBigqueryOptions);
    when(mockBigquery.getDataset(any(DatasetId.class))).thenReturn(Mockito.mock(Dataset.class));
    when(mockBigquery.getTable(any(TableId.class))).thenReturn(Mockito.mock(Table.class));
    when(mockBigquery.update(any(TableInfo.class)))
        .thenThrow(new BigQueryException(-1, "Test BigQueryException"));

    SchemaUtil.setupBigQuery(storageSpec, entitySpec, featureSpecs, mockBigquery);
  }

  /**
   * Expect exceptions when the entity value specified in entitySpec is different from that in any
   * of featureSpecs
   */
  @Test(expected = IllegalArgumentException.class)
  public void setupBigQueryWithInconsistentEntityInSpec() {
    // Mock BigQuery service
    BigQuery mockBigquery = Mockito.mock(BigQuery.class);
    BigQueryOptions mockBigqueryOptions = Mockito.mock(BigQueryOptions.class);
    when(mockBigqueryOptions.getProjectId()).thenReturn("test_project");
    when(mockBigquery.getOptions()).thenReturn(mockBigqueryOptions);
    entitySpec = entitySpec.toBuilder().setName("inconsitent_entity_name").build();
    SchemaUtil.setupBigQuery(storageSpec, entitySpec, featureSpecs, mockBigquery);
  }
}
