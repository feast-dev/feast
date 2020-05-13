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
package feast.core.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.client.util.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse.Status;
import feast.core.CoreServiceProto.GetFeatureSetRequest;
import feast.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListStoresRequest;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.ProjectRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.*;
import feast.types.ValueProto.ValueType.Enum;
import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.tensorflow.metadata.v0.BoolDomain;
import org.tensorflow.metadata.v0.FeaturePresence;
import org.tensorflow.metadata.v0.FeaturePresenceWithinGroup;
import org.tensorflow.metadata.v0.FixedShape;
import org.tensorflow.metadata.v0.FloatDomain;
import org.tensorflow.metadata.v0.ImageDomain;
import org.tensorflow.metadata.v0.IntDomain;
import org.tensorflow.metadata.v0.MIDDomain;
import org.tensorflow.metadata.v0.NaturalLanguageDomain;
import org.tensorflow.metadata.v0.StringDomain;
import org.tensorflow.metadata.v0.StructDomain;
import org.tensorflow.metadata.v0.TimeDomain;
import org.tensorflow.metadata.v0.TimeOfDayDomain;
import org.tensorflow.metadata.v0.URLDomain;
import org.tensorflow.metadata.v0.ValueCount;

public class SpecServiceTest {

  @Mock private FeatureSetRepository featureSetRepository;

  @Mock private StoreRepository storeRepository;

  @Mock private ProjectRepository projectRepository;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private SpecService specService;
  private List<FeatureSet> featureSets;
  private List<Store> stores;
  private Source defaultSource;

  // TODO: Updates update features in place, so if tests follow the wrong order they might break.
  // Refactor this maybe?
  @Before
  public void setUp() {
    initMocks(this);
    defaultSource = TestObjectFactory.defaultSource;

    FeatureSet featureSet1 = newDummyFeatureSet("f1", "project1");
    FeatureSet featureSet2 = newDummyFeatureSet("f2", "project1");

    Feature f3f1 = TestObjectFactory.CreateFeature("f3f1", Enum.INT64);
    Feature f3f2 = TestObjectFactory.CreateFeature("f3f2", Enum.INT64);
    Entity f3e1 = TestObjectFactory.CreateEntity("f3e1", Enum.STRING);
    FeatureSet featureSet3v1 =
        TestObjectFactory.CreateFeatureSet(
            "f3", "project1", Arrays.asList(f3e1), Arrays.asList(f3f2, f3f1));

    featureSets = Arrays.asList(featureSet1, featureSet2);
    when(featureSetRepository.findAll()).thenReturn(featureSets);
    when(featureSetRepository.findAllByOrderByNameAsc()).thenReturn(featureSets);
    when(featureSetRepository.findFeatureSetByNameAndProject_Name("f1", "project1"))
        .thenReturn(featureSets.get(0));
    when(featureSetRepository.findFeatureSetByNameAndProject_Name("f2", "project1"))
        .thenReturn(featureSets.get(1));
    when(featureSetRepository.findAllByNameLikeAndProject_NameOrderByNameAsc("f1", "project1"))
        .thenReturn(featureSets.subList(0, 1));
    when(featureSetRepository.findAllByNameLikeAndProject_NameOrderByNameAsc("asd", "project1"))
        .thenReturn(Lists.newArrayList());
    when(featureSetRepository.findAllByNameLikeAndProject_NameOrderByNameAsc("f%", "project1"))
        .thenReturn(featureSets);
    when(featureSetRepository.findAllByNameLikeAndProject_NameLikeOrderByNameAsc("%", "%"))
        .thenReturn(featureSets);

    when(projectRepository.findAllByArchivedIsFalse())
        .thenReturn(Collections.singletonList(new Project("project1")));
    when(projectRepository.findById("project1")).thenReturn(Optional.of(new Project("project1")));
    Project archivedProject = new Project("archivedproject");
    archivedProject.setArchived(true);
    when(projectRepository.findById(archivedProject.getName()))
        .thenReturn(Optional.of(archivedProject));

    Store store1 = newDummyStore("SERVING");
    Store store2 = newDummyStore("WAREHOUSE");
    stores = Arrays.asList(store1, store2);
    when(storeRepository.findAll()).thenReturn(stores);
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(store1));
    when(storeRepository.findById("NOTFOUND")).thenReturn(Optional.empty());

    specService =
        new SpecService(featureSetRepository, storeRepository, projectRepository, defaultSource);
  }

  @Test
  public void shouldGetAllFeatureSetsIfOnlyWildcardsProvided()
      throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("*").setProject("*").build());
    List<FeatureSetProto.FeatureSet> list = new ArrayList<>();
    for (FeatureSet featureSet : featureSets) {
      FeatureSetProto.FeatureSet toProto = featureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void listFeatureSetShouldFailIfFeatureSetProvidedWithoutProject()
      throws InvalidProtocolBufferException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Invalid listFeatureSetRequest, missing arguments. Must provide project and feature set name.");
    specService.listFeatureSets(Filter.newBuilder().setFeatureSetName("f1").build());
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingNameWithWildcardSearch()
      throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(
            Filter.newBuilder().setProject("project1").setFeatureSetName("f*").build());
    List<FeatureSet> expectedFeatureSets =
        featureSets.stream()
            .filter(fs -> fs.getName().startsWith("f"))
            .collect(Collectors.toList());
    List<FeatureSetProto.FeatureSet> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetProto.FeatureSet toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetFeatureSetsByNameAndProject() throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(
            Filter.newBuilder().setProject("project1").setFeatureSetName("f1").build());
    List<FeatureSet> expectedFeatureSets =
        featureSets.stream().filter(fs -> fs.getName().equals("f1")).collect(Collectors.toList());
    List<FeatureSetProto.FeatureSet> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetProto.FeatureSet toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowExceptionGivenMissingFeatureSetName()
      throws InvalidProtocolBufferException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("No feature set name provided");
    specService.getFeatureSet(GetFeatureSetRequest.newBuilder().build());
  }

  @Test
  public void shouldThrowExceptionGivenMissingFeatureSet() throws InvalidProtocolBufferException {
    expectedException.expect(RetrievalException.class);
    expectedException.expectMessage("Feature set with name \"f1000\" could not be found.");
    specService.getFeatureSet(
        GetFeatureSetRequest.newBuilder().setName("f1000").setProject("project1").build());
  }

  @Test
  public void shouldReturnAllStoresIfNoNameProvided() throws InvalidProtocolBufferException {
    ListStoresResponse actual =
        specService.listStores(ListStoresRequest.Filter.newBuilder().build());
    ListStoresResponse.Builder expected = ListStoresResponse.newBuilder();
    for (Store expectedStore : stores) {
      expected.addStore(expectedStore.toProto());
    }
    assertThat(actual, equalTo(expected.build()));
  }

  @Test
  public void shouldReturnStoreWithName() throws InvalidProtocolBufferException {
    ListStoresResponse actual =
        specService.listStores(ListStoresRequest.Filter.newBuilder().setName("SERVING").build());
    List<Store> expectedStores =
        stores.stream().filter(s -> s.getName().equals("SERVING")).collect(Collectors.toList());
    ListStoresResponse.Builder expected = ListStoresResponse.newBuilder();
    for (Store expectedStore : expectedStores) {
      expected.addStore(expectedStore.toProto());
    }
    assertThat(actual, equalTo(expected.build()));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfNoStoresFoundWithName() {
    expectedException.expect(RetrievalException.class);
    expectedException.expectMessage("Store with name 'NOTFOUND' not found");
    specService.listStores(ListStoresRequest.Filter.newBuilder().setName("NOTFOUND").build());
  }

  @Test
  public void applyFeatureSetShouldReturnFeatureSetIfFeatureSetHasNotChanged()
      throws InvalidProtocolBufferException {
    FeatureSetSpec incomingFeatureSetSpec =
        featureSets.get(0).toProto().getSpec().toBuilder().build();

    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(
            FeatureSetProto.FeatureSet.newBuilder().setSpec(incomingFeatureSetSpec).build());

    verify(featureSetRepository, times(0)).save(ArgumentMatchers.any(FeatureSet.class));
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.NO_CHANGE));
    assertThat(applyFeatureSetResponse.getFeatureSet(), equalTo(featureSets.get(0).toProto()));
  }

  @Test
  public void applyFeatureSetShouldApplyFeatureSetIfNotExists()
      throws InvalidProtocolBufferException {
    when(featureSetRepository.findFeatureSetByNameAndProject_Name("f2", "project1"))
        .thenReturn(null);

    FeatureSetProto.FeatureSet incomingFeatureSet = newDummyFeatureSet("f2", "project1").toProto();

    FeatureSetProto.FeatureSetSpec incomingFeatureSetSpec =
        incomingFeatureSet.getSpec().toBuilder().build();

    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(
            FeatureSetProto.FeatureSet.newBuilder().setSpec(incomingFeatureSet.getSpec()).build());
    verify(projectRepository).saveAndFlush(ArgumentMatchers.any(Project.class));

    FeatureSetProto.FeatureSet expected =
        FeatureSetProto.FeatureSet.newBuilder()
            .setSpec(incomingFeatureSetSpec.toBuilder().setSource(defaultSource.toProto()).build())
            .build();
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.CREATED));
    assertThat(applyFeatureSetResponse.getFeatureSet().getSpec(), equalTo(expected.getSpec()));
  }

  @Test
  public void applyFeatureSetShouldUpdateAndSaveFeatureSetIfAlreadyExists()
      throws InvalidProtocolBufferException {
    FeatureSetProto.FeatureSet incomingFeatureSet = featureSets.get(0).toProto();
    incomingFeatureSet =
        incomingFeatureSet
            .toBuilder()
            .setMeta(incomingFeatureSet.getMeta())
            .setSpec(
                incomingFeatureSet
                    .getSpec()
                    .toBuilder()
                    .addFeatures(
                        FeatureSpec.newBuilder().setName("feature2").setValueType(Enum.STRING))
                    .build())
            .build();

    FeatureSetProto.FeatureSet expected =
        incomingFeatureSet
            .toBuilder()
            .setMeta(incomingFeatureSet.getMeta().toBuilder().build())
            .setSpec(
                incomingFeatureSet.getSpec().toBuilder().setSource(defaultSource.toProto()).build())
            .build();

    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(incomingFeatureSet);
    verify(projectRepository).saveAndFlush(ArgumentMatchers.any(Project.class));
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.UPDATED));
    assertEquals(
        FeatureSet.fromProto(applyFeatureSetResponse.getFeatureSet()),
        FeatureSet.fromProto(expected));
  }

  @Test
  public void applyFeatureSetShouldNotCreateFeatureSetIfFieldsUnordered()
      throws InvalidProtocolBufferException {

    FeatureSet featureSet = featureSets.get(1);
    List<Feature> features = Lists.newArrayList(featureSet.getFeatures());
    Collections.shuffle(features);
    featureSet.setFeatures(Set.copyOf(features));
    FeatureSetProto.FeatureSet incomingFeatureSet = featureSet.toProto();

    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(incomingFeatureSet);
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.NO_CHANGE));
    assertThat(
        applyFeatureSetResponse.getFeatureSet().getSpec().getMaxAge(),
        equalTo(incomingFeatureSet.getSpec().getMaxAge()));
    assertThat(
        applyFeatureSetResponse.getFeatureSet().getSpec().getEntities(0),
        equalTo(incomingFeatureSet.getSpec().getEntities(0)));
    assertThat(
        applyFeatureSetResponse.getFeatureSet().getSpec().getName(),
        equalTo(incomingFeatureSet.getSpec().getName()));
  }

  @Test
  public void applyFeatureSetShouldAcceptPresenceShapeAndDomainConstraints()
      throws InvalidProtocolBufferException {
    List<EntitySpec> entitySpecs = new ArrayList<>();
    entitySpecs.add(EntitySpec.newBuilder().setName("entity1").setValueType(Enum.INT64).build());
    entitySpecs.add(EntitySpec.newBuilder().setName("entity2").setValueType(Enum.INT64).build());
    entitySpecs.add(EntitySpec.newBuilder().setName("entity3").setValueType(Enum.FLOAT).build());
    entitySpecs.add(EntitySpec.newBuilder().setName("entity4").setValueType(Enum.STRING).build());
    entitySpecs.add(EntitySpec.newBuilder().setName("entity5").setValueType(Enum.BOOL).build());

    List<FeatureSpec> featureSpecs = new ArrayList<>();
    featureSpecs.add(
        FeatureSpec.newBuilder()
            .setName("feature1")
            .setValueType(Enum.INT64)
            .setPresence(FeaturePresence.getDefaultInstance())
            .setShape(FixedShape.getDefaultInstance())
            .setDomain("mydomain")
            .build());
    featureSpecs.add(
        FeatureSpec.newBuilder()
            .setName("feature2")
            .setValueType(Enum.INT64)
            .setGroupPresence(FeaturePresenceWithinGroup.getDefaultInstance())
            .setValueCount(ValueCount.getDefaultInstance())
            .setIntDomain(IntDomain.getDefaultInstance())
            .build());
    featureSpecs.add(
        FeatureSpec.newBuilder()
            .setName("feature3")
            .setValueType(Enum.FLOAT)
            .setPresence(FeaturePresence.getDefaultInstance())
            .setValueCount(ValueCount.getDefaultInstance())
            .setFloatDomain(FloatDomain.getDefaultInstance())
            .build());
    featureSpecs.add(
        FeatureSpec.newBuilder()
            .setName("feature4")
            .setValueType(Enum.STRING)
            .setPresence(FeaturePresence.getDefaultInstance())
            .setValueCount(ValueCount.getDefaultInstance())
            .setStringDomain(StringDomain.getDefaultInstance())
            .build());
    featureSpecs.add(
        FeatureSpec.newBuilder()
            .setName("feature5")
            .setValueType(Enum.BOOL)
            .setPresence(FeaturePresence.getDefaultInstance())
            .setValueCount(ValueCount.getDefaultInstance())
            .setBoolDomain(BoolDomain.getDefaultInstance())
            .build());

    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setProject("project1")
            .setName("featureSetWithConstraints")
            .addAllEntities(entitySpecs)
            .addAllFeatures(featureSpecs)
            .build();
    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpec).build();

    ApplyFeatureSetResponse applyFeatureSetResponse = specService.applyFeatureSet(featureSet);
    FeatureSetSpec appliedFeatureSetSpec = applyFeatureSetResponse.getFeatureSet().getSpec();

    // appliedEntitySpecs needs to be sorted because the list returned by specService may not
    // follow the order in the request
    List<EntitySpec> appliedEntitySpecs = new ArrayList<>(appliedFeatureSetSpec.getEntitiesList());
    appliedEntitySpecs.sort(Comparator.comparing(EntitySpec::getName));

    // appliedFeatureSpecs needs to be sorted because the list returned by specService may not
    // follow the order in the request
    List<FeatureSpec> appliedFeatureSpecs =
        new ArrayList<>(appliedFeatureSetSpec.getFeaturesList());
    appliedFeatureSpecs.sort(Comparator.comparing(FeatureSpec::getName));

    assertEquals(appliedEntitySpecs, entitySpecs);
    assertEquals(appliedFeatureSpecs, featureSpecs);
  }

  @Test
  public void applyFeatureSetShouldUpdateFeatureSetWhenConstraintsAreUpdated()
      throws InvalidProtocolBufferException {

    // Map of constraint field name -> value, e.g. "shape" -> FixedShape object.
    // If any of these fields are updated, SpecService should update the FeatureSet.
    Map<String, Object> contraintUpdates = new HashMap<>();
    contraintUpdates.put("presence", FeaturePresence.newBuilder().setMinFraction(0.5).build());
    contraintUpdates.put(
        "group_presence", FeaturePresenceWithinGroup.newBuilder().setRequired(true).build());
    contraintUpdates.put("shape", FixedShape.getDefaultInstance());
    contraintUpdates.put("value_count", ValueCount.newBuilder().setMin(2).build());
    contraintUpdates.put("domain", "new_domain");
    contraintUpdates.put("int_domain", IntDomain.newBuilder().setMax(100).build());
    contraintUpdates.put("float_domain", FloatDomain.newBuilder().setMin(-0.5f).build());
    contraintUpdates.put("string_domain", StringDomain.newBuilder().addValue("string1").build());
    contraintUpdates.put("bool_domain", BoolDomain.newBuilder().setFalseValue("falsy").build());
    contraintUpdates.put("struct_domain", StructDomain.getDefaultInstance());
    contraintUpdates.put("natural_language_domain", NaturalLanguageDomain.getDefaultInstance());
    contraintUpdates.put("image_domain", ImageDomain.getDefaultInstance());
    contraintUpdates.put("mid_domain", MIDDomain.getDefaultInstance());
    contraintUpdates.put("url_domain", URLDomain.getDefaultInstance());
    contraintUpdates.put(
        "time_domain", TimeDomain.newBuilder().setStringFormat("string_format").build());
    contraintUpdates.put("time_of_day_domain", TimeOfDayDomain.getDefaultInstance());

    for (Entry<String, Object> constraint : contraintUpdates.entrySet()) {
      FeatureSet featureSet = newDummyFeatureSet("constraints", "project1");
      FeatureSetProto.FeatureSet existingFeatureSet = featureSet.toProto();
      when(featureSetRepository.findFeatureSetByNameAndProject_Name("constraints", "project1"))
          .thenReturn(featureSet);
      String name = constraint.getKey();
      Object value = constraint.getValue();
      FeatureSpec newFeatureSpec =
          existingFeatureSet
              .getSpec()
              .getFeatures(0)
              .toBuilder()
              .setField(FeatureSpec.getDescriptor().findFieldByName(name), value)
              .build();
      FeatureSetSpec newFeatureSetSpec =
          existingFeatureSet.getSpec().toBuilder().setFeatures(0, newFeatureSpec).build();
      FeatureSetProto.FeatureSet newFeatureSet =
          existingFeatureSet.toBuilder().setSpec(newFeatureSetSpec).build();

      ApplyFeatureSetResponse response = specService.applyFeatureSet(newFeatureSet);

      assertEquals(
          "Response should have CREATED status when field '" + name + "' is updated",
          Status.UPDATED,
          response.getStatus());
      assertEquals(
          "Feature should have field '" + name + "' set correctly",
          constraint.getValue(),
          response
              .getFeatureSet()
              .getSpec()
              .getFeatures(0)
              .getField(FeatureSpec.getDescriptor().findFieldByName(name)));
    }
  }

  @Test
  public void applyFeatureSetShouldCreateProjectWhenNotAlreadyExists()
      throws InvalidProtocolBufferException {
    Feature f3f1 = TestObjectFactory.CreateFeature("f3f1", Enum.INT64);
    Feature f3f2 = TestObjectFactory.CreateFeature("f3f2", Enum.INT64);
    Entity f3e1 = TestObjectFactory.CreateEntity("f3e1", Enum.STRING);
    FeatureSetProto.FeatureSet incomingFeatureSet =
        TestObjectFactory.CreateFeatureSet(
                "f3", "project", Arrays.asList(f3e1), Arrays.asList(f3f2, f3f1))
            .toProto();

    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(incomingFeatureSet);
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.CREATED));
    assertThat(
        applyFeatureSetResponse.getFeatureSet().getSpec().getProject(),
        equalTo(incomingFeatureSet.getSpec().getProject()));
  }

  @Test
  public void applyFeatureSetShouldFailWhenProjectIsArchived()
      throws InvalidProtocolBufferException {
    Feature f3f1 = TestObjectFactory.CreateFeature("f3f1", Enum.INT64);
    Feature f3f2 = TestObjectFactory.CreateFeature("f3f2", Enum.INT64);
    Entity f3e1 = TestObjectFactory.CreateEntity("f3e1", Enum.STRING);
    FeatureSetProto.FeatureSet incomingFeatureSet =
        TestObjectFactory.CreateFeatureSet(
                "f3", "archivedproject", Arrays.asList(f3e1), Arrays.asList(f3f2, f3f1))
            .toProto();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Project is archived");
    specService.applyFeatureSet(incomingFeatureSet);
  }

  @Test
  public void applyFeatureSetShouldAcceptFeatureLabels() throws InvalidProtocolBufferException {
    List<EntitySpec> entitySpecs = new ArrayList<>();
    entitySpecs.add(EntitySpec.newBuilder().setName("entity1").setValueType(Enum.INT64).build());

    Map<String, String> featureLabels0 =
        new HashMap<>() {
          {
            put("label1", "feast1");
          }
        };

    Map<String, String> featureLabels1 =
        new HashMap<>() {
          {
            put("label1", "feast1");
            put("label2", "feast2");
          }
        };

    List<Map<String, String>> featureLabels = new ArrayList<>();
    featureLabels.add(featureLabels0);
    featureLabels.add(featureLabels1);

    List<FeatureSpec> featureSpecs = new ArrayList<>();
    featureSpecs.add(
        FeatureSpec.newBuilder()
            .setName("feature1")
            .setValueType(Enum.INT64)
            .putAllLabels(featureLabels.get(0))
            .build());
    featureSpecs.add(
        FeatureSpec.newBuilder()
            .setName("feature2")
            .setValueType(Enum.INT64)
            .putAllLabels(featureLabels.get(1))
            .build());

    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setProject("project1")
            .setName("featureSetWithConstraints")
            .addAllEntities(entitySpecs)
            .addAllFeatures(featureSpecs)
            .build();
    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpec).build();

    ApplyFeatureSetResponse applyFeatureSetResponse = specService.applyFeatureSet(featureSet);
    FeatureSetSpec appliedFeatureSetSpec = applyFeatureSetResponse.getFeatureSet().getSpec();

    // appliedEntitySpecs needs to be sorted because the list returned by specService may not
    // follow the order in the request
    List<EntitySpec> appliedEntitySpecs = new ArrayList<>(appliedFeatureSetSpec.getEntitiesList());
    appliedEntitySpecs.sort(Comparator.comparing(EntitySpec::getName));

    // appliedFeatureSpecs needs to be sorted because the list returned by specService may not
    // follow the order in the request
    List<FeatureSpec> appliedFeatureSpecs =
        new ArrayList<>(appliedFeatureSetSpec.getFeaturesList());
    appliedFeatureSpecs.sort(Comparator.comparing(FeatureSpec::getName));

    var featureSpecsLabels =
        featureSpecs.stream().map(e -> e.getLabelsMap()).collect(Collectors.toList());
    assertEquals(appliedEntitySpecs, entitySpecs);
    assertEquals(appliedFeatureSpecs, featureSpecs);
    assertEquals(featureSpecsLabels, featureLabels);
  }

  @Test
  public void applyFeatureSetShouldAcceptFeatureSetLabels() throws InvalidProtocolBufferException {
    Map<String, String> featureSetLabels =
        new HashMap<>() {
          {
            put("description", "My precious feature set");
          }
        };

    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setProject("project1")
            .setName("preciousFeatureSet")
            .putAllLabels(featureSetLabels)
            .build();
    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpec).build();

    ApplyFeatureSetResponse applyFeatureSetResponse = specService.applyFeatureSet(featureSet);
    FeatureSetSpec appliedFeatureSetSpec = applyFeatureSetResponse.getFeatureSet().getSpec();

    var appliedLabels = appliedFeatureSetSpec.getLabelsMap();

    assertEquals(featureSetLabels, appliedLabels);
  }

  @Test
  public void shouldUpdateStoreIfConfigChanges() throws InvalidProtocolBufferException {
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(stores.get(0)));
    StoreProto.Store newStore =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder())
            .addSubscriptions(Subscription.newBuilder().setProject("project1").setName("a"))
            .build();
    UpdateStoreResponse actual =
        specService.updateStore(UpdateStoreRequest.newBuilder().setStore(newStore).build());
    UpdateStoreResponse expected =
        UpdateStoreResponse.newBuilder()
            .setStore(newStore)
            .setStatus(UpdateStoreResponse.Status.UPDATED)
            .build();
    ArgumentCaptor<Store> argumentCaptor = ArgumentCaptor.forClass(Store.class);
    verify(storeRepository, times(1)).save(argumentCaptor.capture());
    assertThat(argumentCaptor.getValue().toProto(), equalTo(newStore));
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldDoNothingIfNoChange() throws InvalidProtocolBufferException {
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(stores.get(0)));
    UpdateStoreResponse actual =
        specService.updateStore(
            UpdateStoreRequest.newBuilder().setStore(stores.get(0).toProto()).build());
    UpdateStoreResponse expected =
        UpdateStoreResponse.newBuilder()
            .setStore(stores.get(0).toProto())
            .setStatus(UpdateStoreResponse.Status.NO_CHANGE)
            .build();
    verify(storeRepository, times(0)).save(ArgumentMatchers.any());
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldFailIfGetFeatureSetWithoutProject() throws InvalidProtocolBufferException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("No project provided");
    specService.getFeatureSet(GetFeatureSetRequest.newBuilder().setName("f1").build());
  }

  private FeatureSet newDummyFeatureSet(String name, String project) {
    FeatureSpec f1 =
        FeatureSpec.newBuilder()
            .setName("feature")
            .setValueType(Enum.STRING)
            .putLabels("key", "value")
            .build();
    Feature feature = Feature.fromProto(f1);
    Entity entity = TestObjectFactory.CreateEntity("entity", Enum.STRING);

    FeatureSet fs =
        TestObjectFactory.CreateFeatureSet(
            name, project, Arrays.asList(entity), Arrays.asList(feature));
    fs.setCreated(Date.from(Instant.ofEpochSecond(10L)));
    return fs;
  }

  private Store newDummyStore(String name) {
    // Add type to this method when we enable filtering by type
    Store store = new Store();
    store.setName(name);
    store.setType(StoreType.REDIS.toString());
    store.setSubscriptions("*:*");
    store.setConfig(RedisConfig.newBuilder().setPort(6379).build().toByteArray());
    return store;
  }
}
