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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.dao.EntityInfoRepository;
import feast.core.dao.FeatureGroupInfoRepository;
import feast.core.dao.FeatureInfoRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.FeatureStreamTopic;
import feast.core.model.StorageInfo;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureGroupSpecProto.FeatureGroupSpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import feast.types.ValueProto.ValueType.Enum;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

public class SpecServiceTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  @Mock
  EntityInfoRepository entityInfoRepository;
  @Mock
  FeatureInfoRepository featureInfoRepository;
  @Mock
  FeatureGroupInfoRepository featureGroupInfoRepository;
  @Mock
  FeatureStreamService featureStreamService;
  @Mock
  JobCoordinatorService jobCoordinatorService;
  @Mock
  StorageSpecs storageSpecs;

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
    feature.setId(Strings.lenientFormat("entity.%s", name));
    feature.setName(name);
    feature.setEntity(newTestEntityInfo("entity"));
    feature.setDescription("");
    feature.setOwner("@test");
    feature.setValueType(ValueType.Enum.BOOL);
    feature.setUri("");
    return feature;
  }

  @Test
  public void shouldGetEntitiesMatchingIds() {
    EntityInfo entity1 = newTestEntityInfo("entity1");
    EntityInfo entity2 = newTestEntityInfo("entity2");

    ArrayList<String> ids = Lists.newArrayList("entity1", "entity2");
    when(entityInfoRepository.findAllById(any(Iterable.class)))
        .thenReturn(Lists.newArrayList(entity1, entity2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    List<EntityInfo> actual = specService.getEntities(ids);
    List<EntityInfo> expected = Lists.newArrayList(entity1, entity2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldDeduplicateGetEntities() {
    EntityInfo entity1 = newTestEntityInfo("entity1");
    EntityInfo entity2 = newTestEntityInfo("entity2");

    ArrayList<String> ids = Lists.newArrayList("entity1", "entity2", "entity2");
    when(entityInfoRepository.findAllById(any(Iterable.class)))
        .thenReturn(Lists.newArrayList(entity1, entity2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
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
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);

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
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);

    List<EntityInfo> actual = specService.listEntities();
    List<EntityInfo> expected = Lists.newArrayList(entity1, entity2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetFeaturesMatchingIds() {
    FeatureInfo feature1 = newTestFeatureInfo("feature1");
    FeatureInfo feature2 = newTestFeatureInfo("feature2");

    ArrayList<String> ids = Lists.newArrayList("entity.feature1", "entity.feature2");
    when(featureInfoRepository.findAllById(any(Iterable.class)))
        .thenReturn(Lists.newArrayList(feature1, feature2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    List<FeatureInfo> actual = specService.getFeatures(ids);
    List<FeatureInfo> expected = Lists.newArrayList(feature1, feature2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldDeduplicateGetFeature() {
    FeatureInfo feature1 = newTestFeatureInfo("feature1");
    FeatureInfo feature2 = newTestFeatureInfo("feature2");

    ArrayList<String> ids = Lists
        .newArrayList("entity.feature1", "entity.feature2", "entity.feature2");
    when(featureInfoRepository.findAllById(any(Iterable.class)))
        .thenReturn(Lists.newArrayList(feature1, feature2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    List<FeatureInfo> actual = specService.getFeatures(ids);
    List<FeatureInfo> expected = Lists.newArrayList(feature1, feature2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfAnyFeatureNotFound() {
    FeatureInfo feature2 = newTestFeatureInfo("feature2");

    ArrayList<String> ids = Lists.newArrayList("entity.feature1", "entity.feature2");
    when(featureInfoRepository.findAllById(ids)).thenReturn(Lists.newArrayList(feature2));
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    exception.expect(RetrievalException.class);
    exception.expectMessage("unable to retrieve all features requested: " + ids);
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
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    List<FeatureInfo> actual = specService.listFeatures();
    List<FeatureInfo> expected = Lists.newArrayList(feature1, feature2);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetStorageMatchingIds() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");
    StorageInfo bqStorage = newTestStorageInfo("BIGQUERY1", "BIGQUERY");
    when(storageSpecs.getServingStorageSpec()).thenReturn(redisStorage.getStorageSpec());
    when(storageSpecs.getWarehouseStorageSpec()).thenReturn(bqStorage.getStorageSpec());

    ArrayList<String> ids = Lists.newArrayList("REDIS1", "BIGQUERY1");
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    List<StorageInfo> actual = specService.getStorage(ids);
    List<StorageInfo> expected = Lists.newArrayList(redisStorage, bqStorage);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldDeduplicateGetStorage() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");
    StorageInfo bqStorage = newTestStorageInfo("BIGQUERY1", "BIGQUERY");
    when(storageSpecs.getServingStorageSpec()).thenReturn(redisStorage.getStorageSpec());
    when(storageSpecs.getWarehouseStorageSpec()).thenReturn(bqStorage.getStorageSpec());
    ArrayList<String> ids = Lists.newArrayList("REDIS1", "BIGQUERY1", "BIGQUERY1");

    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    List<StorageInfo> actual = specService.getStorage(ids);
    List<StorageInfo> expected = Lists.newArrayList(redisStorage, bqStorage);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfAnyStorageNotFound() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");
    when(storageSpecs.getServingStorageSpec()).thenReturn(redisStorage.getStorageSpec());

    ArrayList<String> ids = Lists.newArrayList("REDIS1", "BIGQUERY1");
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);

    exception.expect(RetrievalException.class);
    exception.expectMessage("unable to retrieve all storage requested: " + ids);
    specService.getStorage(ids);
  }

  @Test
  public void shouldListAllStorageRegistered() {
    StorageInfo redisStorage = newTestStorageInfo("REDIS1", "REDIS");
    StorageInfo bqStorage = newTestStorageInfo("BIGQUERY1", "BIGQUERY");
    when(storageSpecs.getServingStorageSpec()).thenReturn(redisStorage.getStorageSpec());
    when(storageSpecs.getWarehouseStorageSpec()).thenReturn(bqStorage.getStorageSpec());

    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    List<StorageInfo> actual = specService.listStorage();
    List<StorageInfo> expected = Lists.newArrayList(redisStorage, bqStorage);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldRegisterFeatureGroup() {
    FeatureGroupSpec spec =
        FeatureGroupSpec.newBuilder()
            .setId("group")
            .addTags("tag")
            .build();
    FeatureGroupInfo expectedFeatureGroupInfo = new FeatureGroupInfo(spec);

    when(featureGroupInfoRepository.saveAndFlush(expectedFeatureGroupInfo))
        .thenReturn(expectedFeatureGroupInfo);
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    FeatureGroupInfo actual = specService.applyFeatureGroup(spec);
    assertThat(actual, equalTo(expectedFeatureGroupInfo));
  }

  @Test
  public void shouldRegisterNewEntityAndProvisionTopic() {
    EntitySpec spec =
        EntitySpec.newBuilder()
            .setName("entity")
            .setDescription("description")
            .addTags("tag")
            .build();
    EntityInfo entityInfo = new EntityInfo(spec);
    when(entityInfoRepository.save(entityInfo)).thenReturn(entityInfo);
    when(entityInfoRepository.getOne(spec.getName())).thenReturn(entityInfo);
    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);
    EntityInfo actual = specService.applyEntity(spec);
    verify(featureStreamService).provisionTopic(entityInfo);
    assertThat(actual, equalTo(entityInfo));
  }

  @Test
  public void shouldRegisterMultipleValidFeaturesAndStartJob() {
    FeatureSpec f1 = FeatureSpec.newBuilder()
        .setId("entity.feature1")
        .setName("feature1")
        .setEntity("entity")
        .setValueType(Enum.BOOL)
        .build();
    FeatureSpec f2 = FeatureSpec.newBuilder()
        .setId("entity.feature2")
        .setName("feature2")
        .setEntity("entity")
        .setValueType(Enum.BOOL)
        .build();
    FeatureStreamTopic featureStreamTopic = new FeatureStreamTopic();
    featureStreamTopic.setName("feast-entity-features");
    EntityInfo entityInfo = new EntityInfo("entity", "", "",featureStreamTopic, Lists.newArrayList(),true);
    entityInfo.setName("entity");
    entityInfo.setTopic(featureStreamTopic);

    List<FeatureSpec> specs = Lists.newArrayList(f1, f2);
    List<FeatureInfo> expected = Lists.newArrayList(new FeatureInfo(f1, entityInfo, null), new FeatureInfo(f2, entityInfo, null));

    when(featureInfoRepository.findById(ArgumentMatchers.any())).thenReturn(Optional.empty());
    when(entityInfoRepository.findById("entity")).thenReturn(Optional.of(entityInfo));
    when(featureInfoRepository.save(any(FeatureInfo.class))).thenAnswer(i -> i.getArguments()[0]);
    when(featureInfoRepository.findByEntityName("entity")).thenReturn(expected);
    when(storageSpecs.getErrorsStorageSpec()).thenReturn(StorageSpec.newBuilder().setId("err").build());
    when(storageSpecs.getServingStorageSpec()).thenReturn(StorageSpec.newBuilder().setId("serving").build());

    SpecService specService =
        new SpecService(
            entityInfoRepository,
            featureInfoRepository,
            featureGroupInfoRepository,
            featureStreamService,
            jobCoordinatorService,
            storageSpecs);

    List<FeatureInfo> actual = specService.applyFeatures(specs);

    verify(jobCoordinatorService, times(1)).startJob(any());
    assertEquals(expected, actual);
  }
}

