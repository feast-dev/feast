/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.core.service;

import static com.jayway.jsonassert.impl.matcher.IsMapContainingKey.hasKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapWithSize.aMapWithSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import feast.common.it.BaseIT;
import feast.common.it.DataGenerator;
import feast.common.it.SimpleCoreClient;
import feast.common.util.TestUtil;
import feast.proto.core.*;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

@SpringBootTest
public class SpecServiceIT extends BaseIT {

  static CoreServiceGrpc.CoreServiceBlockingStub stub;
  static SimpleCoreClient apiClient;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    stub = CoreServiceGrpc.newBlockingStub(channel);
    apiClient = new SimpleCoreClient(stub);
  }

  private FeatureTableProto.FeatureTableSpec example1;
  private FeatureTableProto.FeatureTableSpec example2;

  @BeforeEach
  public void initState() {

    EntityProto.EntitySpecV2 entitySpec1 =
        DataGenerator.createEntitySpecV2(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key", "label_value"));
    EntityProto.EntitySpecV2 entitySpec2 =
        DataGenerator.createEntitySpecV2(
            "entity2",
            "Entity 2 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key2", "label_value2"));
    apiClient.simpleApplyEntity("default", entitySpec1);
    apiClient.simpleApplyEntity("default", entitySpec2);

    example1 =
        DataGenerator.createFeatureTableSpec(
                "featuretable1",
                Arrays.asList("entity1", "entity2"),
                new HashMap<>() {
                  {
                    put("feature1", ValueProto.ValueType.Enum.STRING);
                    put("feature2", ValueProto.ValueType.Enum.FLOAT);
                  }
                },
                7200,
                ImmutableMap.of("feat_key2", "feat_value2"))
            .toBuilder()
            .setBatchSource(
                DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
            .build();

    example2 =
        DataGenerator.createFeatureTableSpec(
                "featuretable2",
                Arrays.asList("entity1", "entity2"),
                new HashMap<>() {
                  {
                    put("feature3", ValueProto.ValueType.Enum.STRING);
                    put("feature4", ValueProto.ValueType.Enum.FLOAT);
                  }
                },
                7200,
                ImmutableMap.of("feat_key4", "feat_value4"))
            .toBuilder()
            .setBatchSource(
                DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
            .build();

    apiClient.applyFeatureTable("default", example1);
    apiClient.applyFeatureTable("default", example2);
    apiClient.simpleApplyEntity(
        "project1",
        DataGenerator.createEntitySpecV2(
            "entity3",
            "Entity 3 description",
            ValueProto.ValueType.Enum.STRING,
            ImmutableMap.of("label_key2", "label_value2")));
    apiClient.updateStore(DataGenerator.getDefaultStore());
  }

  @Nested
  class ListEntities {
    @Test
    public void shouldFilterEntitiesByLabels() {
      List<EntityProto.Entity> entities =
          apiClient.simpleListEntities("", ImmutableMap.of("label_key2", "label_value2"));

      assertThat(entities, hasSize(1));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity2")))));
    }

    @Test
    public void shouldUseDefaultProjectIfProjectUnspecified() {
      List<EntityProto.Entity> entities = apiClient.simpleListEntities("");

      assertThat(entities, hasSize(2));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity1")))));
    }

    @Test
    public void shouldFilterEntitiesByProjectAndLabels() {
      List<EntityProto.Entity> entities =
          apiClient.simpleListEntities("project1", ImmutableMap.of("label_key2", "label_value2"));

      assertThat(entities, hasSize(1));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity3")))));
    }

    @Test
    public void shouldThrowExceptionGivenWildcardProject() {
      CoreServiceProto.ListEntitiesRequest.Filter filter =
          CoreServiceProto.ListEntitiesRequest.Filter.newBuilder().setProject("default*").build();
      StatusRuntimeException exc =
          assertThrows(StatusRuntimeException.class, () -> apiClient.simpleListEntities(filter));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: invalid value for project resource, %s: "
                      + "argument must only contain alphanumeric characters and underscores.",
                  filter.getProject())));
    }
  }

  @Nested
  class ListFeatureTables {
    @Test
    public void shouldFilterFeatureTablesByProjectAndLabels() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .putAllLabels(ImmutableMap.of("feat_key2", "feat_value2"))
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      assertThat(featureTables, hasSize(1));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable1")))));
    }

    @Test
    public void shouldUseDefaultProjectIfProjectUnspecified() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      assertThat(featureTables, hasSize(2));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable1")))));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable2")))));
    }

    @Test
    public void shouldThrowExceptionGivenWildcardProject() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default*")
              .build();
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleListFeatureTables(filter));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: invalid value for project resource, %s: "
                      + "argument must only contain alphanumeric characters and underscores.",
                  filter.getProject())));
    }
  }

  @Nested
  class ApplyEntity {
    @Test
    public void shouldThrowExceptionGivenEntityWithDash() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyEntity(
                      "default",
                      DataGenerator.createEntitySpecV2(
                          "dash-entity",
                          "Dash Entity description",
                          ValueProto.ValueType.Enum.STRING,
                          ImmutableMap.of("test_key", "test_value"))));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: invalid value for %s resource, %s: %s",
                  "entity",
                  "dash-entity",
                  "argument must only contain alphanumeric characters and underscores.")));
    }

    @Test
    public void shouldThrowExceptionIfTypeChanged() {
      String projectName = "default";

      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "entity1",
              "Entity description",
              ValueProto.ValueType.Enum.FLOAT,
              ImmutableMap.of("label_key", "label_value"));

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleApplyEntity("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: You are attempting to change the type of this entity in %s project from %s to %s. This isn't allowed. Please create a new entity.",
                  "default", "STRING", spec.getValueType())));
    }

    @Test
    public void shouldReturnEntityIfEntityHasNotChanged() {
      String projectName = "default";
      EntityProto.EntitySpecV2 spec = apiClient.simpleGetEntity(projectName, "entity1").getSpec();

      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity(projectName, spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldApplyEntityIfNotExists() {
      String projectName = "default";
      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "new_entity",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              ImmutableMap.of("label_key", "label_value"));

      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity(projectName, spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldCreateProjectWhenNotAlreadyExists() {
      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "new_entity2",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              ImmutableMap.of("key1", "val1"));
      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity("new_project", spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldFailWhenProjectIsArchived() {
      apiClient.createProject("archived");
      apiClient.archiveProject("archived");

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyEntity(
                      "archived",
                      DataGenerator.createEntitySpecV2(
                          "new_entity3",
                          "Entity description",
                          ValueProto.ValueType.Enum.STRING,
                          ImmutableMap.of("key1", "val1"))));
      assertThat(exc.getMessage(), equalTo("INTERNAL: Project is archived: archived"));
    }

    @Test
    public void shouldUpdateLabels() {
      EntityProto.EntitySpecV2 spec =
          DataGenerator.createEntitySpecV2(
              "entity1",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              ImmutableMap.of("label_key", "label_value", "label_key2", "label_value2"));

      CoreServiceProto.ApplyEntityResponse response = apiClient.simpleApplyEntity("default", spec);

      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
    }
  }

  @Nested
  class UpdateStore {
    @Test
    public void shouldUpdateStoreIfConfigChanges() {
      StoreProto.Store defaultStore = DataGenerator.getDefaultStore();

      StoreProto.Store updatedStore =
          DataGenerator.createStore(
              defaultStore.getName(),
              defaultStore.getType(),
              ImmutableList.of(Triple.of("project1", "*", false)));

      CoreServiceProto.UpdateStoreResponse response = apiClient.updateStore(updatedStore);
      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.UpdateStoreResponse.Status.UPDATED));
    }

    @Test
    public void shouldDoNothingIfNoChange() {
      CoreServiceProto.UpdateStoreResponse response =
          apiClient.updateStore(DataGenerator.getDefaultStore());
      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.UpdateStoreResponse.Status.NO_CHANGE));
    }
  }

  @Nested
  class GetEntity {
    @Test
    public void shouldThrowExceptionGivenMissingEntity() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleGetEntity("default", ""));

      assertThat(exc.getMessage(), equalTo("INVALID_ARGUMENT: No entity name provided"));
    }

    public void shouldRetrieveFromDefaultIfProjectNotSpecified() {
      String entityName = "entity1";
      EntityProto.Entity entity = apiClient.simpleGetEntity("", entityName);

      assertThat(entity.getSpec().getName(), equalTo(entityName));
    }
  }

  @Nested
  class GetFeatureTable {
    @Test
    public void shouldThrowExceptionGivenNoSuchFeatureTable() {
      String projectName = "default";
      String featureTableName = "invalid_table";
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () -> apiClient.simpleGetFeatureTable(projectName, featureTableName));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "NOT_FOUND: No such Feature Table: (project: %s, name: %s)",
                  projectName, featureTableName)));
    }

    @Test
    public void shouldReturnFeatureTableIfExists() {
      FeatureTableProto.FeatureTable featureTable =
          apiClient.simpleGetFeatureTable("default", "featuretable1");

      assertTrue(TestUtil.compareFeatureTableSpec(featureTable.getSpec(), example1));
    }
  }

  @Nested
  class ListStores {
    @Test
    public void shouldReturnAllStoresIfNoNameProvided() {
      apiClient.updateStore(DataGenerator.getDefaultStore());
      apiClient.updateStore(
          DataGenerator.createStore(
              "data", StoreProto.Store.StoreType.REDIS, Collections.emptyList()));

      List<StoreProto.Store> actual =
          stub.listStores(
                  CoreServiceProto.ListStoresRequest.newBuilder()
                      .setFilter(CoreServiceProto.ListStoresRequest.Filter.newBuilder().build())
                      .build())
              .getStoreList();

      assertThat(actual, hasSize(2));
      assertThat(actual, hasItem(hasProperty("name", equalTo("test-store"))));
      assertThat(actual, hasItem(hasProperty("name", equalTo("data"))));
    }

    @Test
    public void shouldThrowRetrievalExceptionIfNoStoresFoundWithName() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  stub.listStores(
                      CoreServiceProto.ListStoresRequest.newBuilder()
                          .setFilter(
                              CoreServiceProto.ListStoresRequest.Filter.newBuilder()
                                  .setName("unknown")
                                  .build())
                          .build()));

      assertThat(exc.getMessage(), equalTo("INTERNAL: Store with name 'unknown' not found"));
    }
  }

  @Nested
  class ListFeatures {
    @Test
    public void shouldFilterFeaturesByEntitiesAndLabels() {
      // Case 1: Only filter by entities
      Map<String, FeatureProto.FeatureSpecV2> result1 =
          apiClient.simpleListFeatures("default", "entity1", "entity2");

      assertThat(result1, aMapWithSize(4));
      assertThat(result1, hasKey(equalTo("featuretable1:feature1")));
      assertThat(result1, hasKey(equalTo("featuretable1:feature2")));
      assertThat(result1, hasKey(equalTo("featuretable2:feature3")));
      assertThat(result1, hasKey(equalTo("featuretable2:feature4")));

      // Case 2: Filter by entities and labels
      Map<String, FeatureProto.FeatureSpecV2> result2 =
          apiClient.simpleListFeatures(
              "default",
              ImmutableMap.of("feat_key2", "feat_value2"),
              ImmutableList.of("entity1", "entity2"));

      assertThat(result2, aMapWithSize(2));
      assertThat(result2, hasKey(equalTo("featuretable1:feature1")));
      assertThat(result2, hasKey(equalTo("featuretable1:feature2")));

      // Case 3: Filter by labels
      Map<String, FeatureProto.FeatureSpecV2> result3 =
          apiClient.simpleListFeatures(
              "default", ImmutableMap.of("feat_key4", "feat_value4"), Collections.emptyList());

      assertThat(result3, aMapWithSize(2));
      assertThat(result3, hasKey(equalTo("featuretable2:feature3")));
      assertThat(result3, hasKey(equalTo("featuretable2:feature4")));

      // Case 4: Filter by nothing, except project
      Map<String, FeatureProto.FeatureSpecV2> result4 =
          apiClient.simpleListFeatures("project1", ImmutableMap.of(), Collections.emptyList());

      assertThat(result4, aMapWithSize(0));

      // Case 5: Filter by nothing; will use default project
      Map<String, FeatureProto.FeatureSpecV2> result5 =
          apiClient.simpleListFeatures("", ImmutableMap.of(), Collections.emptyList());

      assertThat(result5, aMapWithSize(4));
      assertThat(result5, hasKey(equalTo("featuretable1:feature1")));
      assertThat(result5, hasKey(equalTo("featuretable1:feature2")));
      assertThat(result5, hasKey(equalTo("featuretable2:feature3")));
      assertThat(result5, hasKey(equalTo("featuretable2:feature4")));

      // Case 6: Filter by mismatched entity
      Map<String, FeatureProto.FeatureSpecV2> result6 =
          apiClient.simpleListFeatures("default", ImmutableMap.of(), ImmutableList.of("entity1"));
      assertThat(result6, aMapWithSize(0));
    }
  }

  @Nested
  public class ApplyFeatureTable {
    private FeatureTableSpec getTestSpec() {
      return example1
          .toBuilder()
          .setName("apply_test")
          .setStreamSource(
              DataGenerator.createKafkaDataSourceSpec(
                  "localhost:9092", "topic", "class.path", "ts_col"))
          .build();
    }

    @Test
    public void shouldApplyNewValidTable() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());

      assertTrue(TestUtil.compareFeatureTableSpec(table.getSpec(), getTestSpec()));
      assertThat(table.getMeta().getRevision(), equalTo(0L));
    }

    @Test
    public void shouldUpdateExistingTableWithValidSpec() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());

      FeatureTableSpec updatedSpec =
          getTestSpec()
              .toBuilder()
              .clearFeatures()
              .addFeatures(
                  DataGenerator.createFeatureSpecV2(
                      "feature5", ValueProto.ValueType.Enum.FLOAT, ImmutableMap.of()))
              .setStreamSource(
                  DataGenerator.createKafkaDataSourceSpec(
                      "localhost:9092", "new_topic", "new.class", "ts_col"))
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
      assertThat(updatedTable.getMeta().getRevision(), equalTo(table.getMeta().getRevision() + 1L));
    }

    @Test
    public void shouldUpdateFeatureTableOnEntityChange() {
      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec().toBuilder().clearEntities().addEntities("entity1").build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldUpdateFeatureTableOnMaxAgeChange() {
      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec()
              .toBuilder()
              .setMaxAge(Duration.newBuilder().setSeconds(600).build())
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldUpdateFeatureTableOnFeatureTypeChange() {
      int featureIdx =
          IntStream.range(0, getTestSpec().getFeaturesCount())
              .filter(i -> getTestSpec().getFeatures(i).getName().equals("feature2"))
              .findFirst()
              .orElse(-1);

      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec()
              .toBuilder()
              .setFeatures(
                  featureIdx,
                  DataGenerator.createFeatureSpecV2(
                      "feature2", ValueProto.ValueType.Enum.STRING_LIST, ImmutableMap.of()))
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldUpdateFeatureTableOnFeatureAddition() {
      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec()
              .toBuilder()
              .addFeatures(
                  DataGenerator.createFeatureSpecV2(
                      "feature6", ValueProto.ValueType.Enum.FLOAT, ImmutableMap.of()))
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldNotUpdateIfNoChanges() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());
      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", getTestSpec());

      assertThat(updatedTable.getMeta().getRevision(), equalTo(table.getMeta().getRevision()));
    }

    @Test
    public void shouldErrorOnMissingBatchSource() {
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "ft",
                  List.of("entity1"),
                  Map.of("event_timestamp", ValueProto.ValueType.Enum.INT64),
                  3600,
                  Map.of())
              .toBuilder()
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo("INVALID_ARGUMENT: FeatureTable batch source cannot be empty."));
    }

    @Test
    public void shouldErrorOnInvalidBigQueryTableRef() {
      String invalidTableRef = "invalid.bq:path";
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "ft",
                  List.of("entity1"),
                  Map.of("feature", ValueProto.ValueType.Enum.INT64),
                  3600,
                  Map.of())
              .toBuilder()
              .setBatchSource(
                  DataGenerator.createBigQueryDataSourceSpec(invalidTableRef, "ts_col", ""))
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: invalid value for FeatureTable resource, %s: argument must be in the form of <project:dataset.table> .",
                  invalidTableRef)));
    }

    @Test
    public void shouldErrorOnReservedNames() {
      // Reserved name used as feature name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("entity1"),
                          Map.of("event_timestamp", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));

      // Reserved name used in as entity name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("created_timestamp"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnInvalidName() {
      // Invalid feature table name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "f-t",
                          List.of("entity1"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));

      // Invalid feature name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("entity1"),
                          Map.of("feature-1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnNotFoundEntityName() {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft1",
                          List.of("entity_not_found"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnArchivedProject() {
      apiClient.createProject("archived");
      apiClient.archiveProject("archived");

      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "archived",
                  DataGenerator.createFeatureTableSpec(
                          "ft1",
                          List.of("entity1", "entity2"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }
  }

  @Nested
  public class DeleteFeatureTable {

    @Test
    public void shouldReturnNoTables() {
      String projectName = "default";
      String featureTableName = "featuretable1";

      apiClient.deleteFeatureTable(projectName, featureTableName);

      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .putLabels("feat_key2", "feat_value2")
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () -> apiClient.simpleGetFeatureTable(projectName, featureTableName));

      assertThat(featureTables.size(), equalTo(0));
      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "NOT_FOUND: Feature Table has been deleted: (project: %s, name: %s)",
                  projectName, featureTableName)));
    }

    @Test
    public void shouldUpdateDeletedTable() {
      String projectName = "default";
      String featureTableName = "featuretable1";

      apiClient.deleteFeatureTable(projectName, featureTableName);

      FeatureTableSpec featureTableSpec =
          DataGenerator.createFeatureTableSpec(
                  featureTableName,
                  Arrays.asList("entity1", "entity2"),
                  new HashMap<>() {
                    {
                      put("feature3", ValueProto.ValueType.Enum.INT64);
                    }
                  },
                  7200,
                  ImmutableMap.of("feat_key3", "feat_value3"))
              .toBuilder()
              .setBatchSource(
                  DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
              .build();

      apiClient.applyFeatureTable(projectName, featureTableSpec);

      FeatureTableProto.FeatureTable featureTable =
          apiClient.simpleGetFeatureTable(projectName, featureTableName);

      assertTrue(TestUtil.compareFeatureTableSpec(featureTable.getSpec(), featureTableSpec));
    }

    @Test
    public void shouldErrorIfTableNotExist() {
      String projectName = "default";
      String featureTableName = "nonexistent_table";
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () -> apiClient.deleteFeatureTable(projectName, featureTableName));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "NOT_FOUND: No such Feature Table: (project: %s, name: %s)",
                  projectName, featureTableName)));
    }
  }
}
