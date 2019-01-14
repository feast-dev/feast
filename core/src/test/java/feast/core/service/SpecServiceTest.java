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

package feast.core.service;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.core.dao.StorageInfoRepository;
import feast.core.exception.RegistrationException;
import feast.core.exception.RetrievalException;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.StorageInfo;
import feast.core.storage.SchemaManager;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.GranularityProto.Granularity;
import feast.types.ValueProto.ValueType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SpecServiceTest {
  @Mock EntityInfoRepository entityInfoRepository;
  @Mock FeatureInfoRepository featureInfoRepository;
  @Mock FeatureGroupInfoRepository featureGroupInfoRepository;
  @Mock StorageInfoRepository storageInfoRepository;
  @Mock SchemaManager schemaManager;

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() {
    initMocks(this);
  }

  private EntityInfo newTestEntityInfo(String name) {
    EntityInfo entity = new EntityInfo();
    entity.setName(name);
    entity.setDescription("testing");
    return entity;
  }

  private StorageInfo newTestStorageInfo(String id, String type) {
    StorageInfo storage = new StorageInfo();
    storage.setId(id);
    storage.setType(type);
    return storage;
  }

  private FeatureInfo newTestFeatureInfo(String name) {
    FeatureInfo feature = new FeatureInfo();
    feature.setId(Strings.lenientFormat("entity.NONE.%s", name));
    feature.setName(name);
    feature.setEntity(newTestEntityInfo("entity"));
    feature.setDescription("");
    feature.setOwner("@test");
    feature.setGranularity(Granularity.Enum.NONE);
    feature.setValueType(ValueType.Enum.BOOL);
    feature.setUri("");
    feature.setWarehouseStore(newTestStorageInfo("BIGQUERY1", "BIGQUERY"));
    feature.setServingStore(newTestStorageInfo("REDIS1", "REDIS"));
    return feature;
  }

  @Test
  public void shouldGetEntitiesMatchingIds() {
    EntityInfo entity1 = newTestEntityInfo("entity1");
    EntityInfo entity2 = newTestEntityInfo("entity2");

    ArrayList<String> ids = Lists.newArrayList("entity1", "entity2");
    when(entityInfoRepository.findAllById(any(Iterable.class))).thenReturn(Lists.newArrayList(entity1, entity2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<EntityInfo> actual = specService.getEntities(ids);
    List<EntityInfo> expected = Lists.newArrayList(entity1, entity2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldDeduplicateGetEntities() {
    EntityInfo entity1 = newTestEntityInfo("entity1");
    EntityInfo entity2 = newTestEntityInfo("entity2");

    ArrayList<String> ids = Lists.newArrayList("entity1", "entity2", "entity2");
    when(entityInfoRepository.findAllById(any(Iterable.class))).thenReturn(Lists.newArrayList(entity1, entity2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<EntityInfo> actual = specService.getEntities(ids);
    List<EntityInfo> expected = Lists.newArrayList(entity1, entity2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfAnyEntityNotFound() {
    EntityInfo entity1 = newTestEntityInfo("entity1");

    ArrayList<String> ids = Lists.newArrayList("entity1", "entity2");
    when(entityInfoRepository.findAllById(ids)).thenReturn(Lists.newArrayList(entity1));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);

    exception.expect(RetrievalException.class);
    exception.expectMessage("unable to retrieve all entities requested");
    specService.getEntities(ids);
  }

  @Test
  public void shouldListAllEntitiesRegistered() {
    EntityInfo entity1 = newTestEntityInfo("entity1");
    EntityInfo entity2 = newTestEntityInfo("entity2");

    when(entityInfoRepository.findAll()).thenReturn(Lists.newArrayList(entity1, entity2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);

    List<EntityInfo> actual = specService.listEntities();
    List<EntityInfo> expected = Lists.newArrayList(entity1, entity2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetFeaturesMatchingIds() {
    FeatureInfo feature1 = newTestFeatureInfo("feature1");
    FeatureInfo feature2 = newTestFeatureInfo("feature2");

    ArrayList<String> ids = Lists.newArrayList("entity.none.feature1", "entity.none.feature2");
    when(featureInfoRepository.findAllById(any(Iterable.class))).thenReturn(Lists.newArrayList(feature1, feature2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<FeatureInfo> actual = specService.getFeatures(ids);
    List<FeatureInfo> expected = Lists.newArrayList(feature1, feature2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldDeduplicateGetFeature() {
    FeatureInfo feature1 = newTestFeatureInfo("feature1");
    FeatureInfo feature2 = newTestFeatureInfo("feature2");

    ArrayList<String> ids = Lists.newArrayList("entity.none.feature1", "entity.none.feature2", "entity.none.feature2");
    when(featureInfoRepository.findAllById(any(Iterable.class))).thenReturn(Lists.newArrayList(feature1, feature2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<FeatureInfo> actual = specService.getFeatures(ids);
    List<FeatureInfo> expected = Lists.newArrayList(feature1, feature2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfAnyFeatureNotFound() {
    FeatureInfo feature2 = newTestFeatureInfo("feature2");

    ArrayList<String> ids = Lists.newArrayList("entity.none.feature1", "entity.none.feature2");
    when(featureInfoRepository.findAllById(ids)).thenReturn(Lists.newArrayList(feature2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    exception.expect(RetrievalException.class);
    exception.expectMessage("unable to retrieve all features requested");
    specService.getFeatures(ids);
  }

  @Test
  public void shouldListAllFeaturesRegistered() {
    FeatureInfo feature1 = newTestFeatureInfo("feature1");
    FeatureInfo feature2 = newTestFeatureInfo("feature2");

    when(featureInfoRepository.findAll()).thenReturn(Lists.newArrayList(feature1, feature2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<FeatureInfo> actual = specService.listFeatures();
    List<FeatureInfo> expected = Lists.newArrayList(feature1, feature2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetStorageMatchingIds() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");
    StorageInfo bqStorage = newTestStorageInfo("BIGQUERY1", "BIGQUERY");

    ArrayList<String> ids = Lists.newArrayList("REDIS1", "BIGQUERY1");
    when(storageInfoRepository.findAllById(any(Iterable.class)))
        .thenReturn(Lists.newArrayList(redisStorage, bqStorage));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<StorageInfo> actual = specService.getStorage(ids);
    List<StorageInfo> expected = Lists.newArrayList(redisStorage, bqStorage);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldDeduplicateGetStorage() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");
    StorageInfo bqStorage = newTestStorageInfo("BIGQUERY1", "BIGQUERY");

    ArrayList<String> ids = Lists.newArrayList("REDIS1", "BIGQUERY1", "BIGQUERY1");
    when(storageInfoRepository.findAllById(any(Iterable.class)))
        .thenReturn(Lists.newArrayList(redisStorage, bqStorage));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<StorageInfo> actual = specService.getStorage(ids);
    List<StorageInfo> expected = Lists.newArrayList(redisStorage, bqStorage);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfAnyStorageNotFound() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");

    ArrayList<String> ids = Lists.newArrayList("REDIS1", "BIGQUERY1");
    when(storageInfoRepository.findAllById(ids)).thenReturn(Lists.newArrayList(redisStorage));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);

    exception.expect(RetrievalException.class);
    exception.expectMessage("unable to retrieve all storage requested");
    specService.getStorage(ids);
  }

  @Test
  public void shouldListAllStorageRegistered() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");
    StorageInfo bqStorage = newTestStorageInfo("BIGQUERY1", "BIGQUERY");

    when(storageInfoRepository.findAll()).thenReturn(Lists.newArrayList(redisStorage, bqStorage));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    List<StorageInfo> actual = specService.listStorage();
    List<StorageInfo> expected = Lists.newArrayList(redisStorage, bqStorage);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldRegisterFeatureWithGroupInheritance() {
    FeatureGroupInfo group = new FeatureGroupInfo();
    group.setId("testGroup");
    group.setServingStore(newTestStorageInfo("REDIS1", "REDIS"));
    group.setWarehouseStore(newTestStorageInfo("BIGQUERY1", "BIGQUERY"));
    when(featureGroupInfoRepository.findById("testGroup")).thenReturn(Optional.of(group));

    EntityInfo entity = new EntityInfo();
    entity.setName("entity");
    when(entityInfoRepository.findById("entity")).thenReturn(Optional.of(entity));

    FeatureSpec spec =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("desc")
            .setEntity("entity")
            .setUri("uri")
            .setGroup("testGroup")
            .setGranularity(Granularity.Enum.NONE)
            .setValueType(ValueType.Enum.BYTES)
            .build();

    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("BIGQUERY1").build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setServing(servingDataStore)
            .setWarehouse(warehouseDataStore)
            .build();

    FeatureSpec resolvedSpec =
        FeatureSpec.newBuilder()
            .setId("entity.none.name")
            .setName("name")
            .setOwner("owner")
            .setDescription("desc")
            .setEntity("entity")
            .setUri("uri")
            .setGroup("testGroup")
            .setGranularity(Granularity.Enum.NONE)
            .setValueType(ValueType.Enum.BYTES)
            .setDataStores(dataStores)
            .build();

    ArgumentCaptor<FeatureSpec> resolvedSpecCaptor = ArgumentCaptor.forClass(FeatureSpec.class);

    FeatureInfo featureInfo = new FeatureInfo(spec, entity, null, null, group);
    when(featureInfoRepository.saveAndFlush(featureInfo)).thenReturn(featureInfo);

    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    FeatureInfo actual = specService.applyFeature(spec);
    verify(schemaManager).registerFeature(resolvedSpecCaptor.capture());

    assertThat(resolvedSpecCaptor.getValue(), equalTo(resolvedSpec));
    assertThat(actual, equalTo(featureInfo));
  }

  @Test
  public void shouldRegisterFeatureGroupIfStoresArePresent() {
    StorageInfo bqStore = newTestStorageInfo("BIGQUERY1", "bigquery");
    StorageInfo redisStore = newTestStorageInfo("REDIS1", "redis");
    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("BIGQUERY1").build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setServing(servingDataStore)
            .setWarehouse(warehouseDataStore)
            .build();
    FeatureGroupSpec spec =
        FeatureGroupSpec.newBuilder()
            .setId("group")
            .addTags("tag")
            .setDataStores(dataStores)
            .build();
    FeatureGroupInfo expectedFeatureGroupInfo = new FeatureGroupInfo(spec, redisStore, bqStore);

    when(storageInfoRepository.findById("BIGQUERY1")).thenReturn(Optional.of(bqStore));
    when(storageInfoRepository.findById("REDIS1")).thenReturn(Optional.of(redisStore));
    when(featureGroupInfoRepository.saveAndFlush(expectedFeatureGroupInfo))
        .thenReturn(expectedFeatureGroupInfo);
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    FeatureGroupInfo actual = specService.applyFeatureGroup(spec);
    assertThat(actual, equalTo(expectedFeatureGroupInfo));
  }

  @Test
  public void shouldThrowRegistrationExceptionWhenRegisteringFeatureGroupIfStoresMissing() {
    StorageInfo bqStore = newTestStorageInfo("BIGQUERY1", "bigquery");
    DataStore servingDataStore = DataStore.newBuilder().setId("REDIS1").build();
    DataStore warehouseDataStore = DataStore.newBuilder().setId("BIGQUERY1").build();
    DataStores dataStores =
        DataStores.newBuilder()
            .setServing(servingDataStore)
            .setWarehouse(warehouseDataStore)
            .build();
    FeatureGroupSpec spec =
        FeatureGroupSpec.newBuilder()
            .setId("group")
            .addTags("tag")
            .setDataStores(dataStores)
            .build();
    when(storageInfoRepository.findById("BIGQUERY1")).thenReturn(Optional.of(bqStore));
    when(storageInfoRepository.findById("REDIS1")).thenReturn(Optional.empty());

    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);

    exception.expect(RegistrationException.class);
    specService.applyFeatureGroup(spec);
  }

  @Test
  public void shouldRegisterEntity() {
    EntitySpec spec =
        EntitySpec.newBuilder()
            .setName("entity")
            .setDescription("description")
            .addTags("tag")
            .build();
    EntityInfo entityInfo = new EntityInfo(spec);
    when(entityInfoRepository.saveAndFlush(entityInfo)).thenReturn(entityInfo);
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    EntityInfo actual = specService.applyEntity(spec);
    assertThat(actual, equalTo(entityInfo));
  }

  @Test
  public void shouldRegisterStorage() {
    StorageSpec spec = StorageSpec.newBuilder().setId("REDIS1").setType("redis").build();
    StorageInfo storageInfo = new StorageInfo(spec);
    when(storageInfoRepository.saveAndFlush(storageInfo)).thenReturn(storageInfo);
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            storageInfoRepository,
            featureGroupInfoRepository,
            schemaManager);
    StorageInfo actual = specService.registerStorage(spec);
    assertThat(actual, equalTo(storageInfo));
  }

}
