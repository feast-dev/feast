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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.client.util.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse;
import feast.core.CoreServiceProto.ApplyFeatureSetResponse.Status;
import feast.core.CoreServiceProto.GetFeatureSetRequest;
import feast.core.CoreServiceProto.GetFeatureSetResponse;
import feast.core.CoreServiceProto.ListFeatureSetsRequest.Filter;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListStoresRequest;
import feast.core.CoreServiceProto.ListStoresResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.FeatureSet;
import feast.core.model.Field;
import feast.core.model.Source;
import feast.core.model.Store;
import feast.types.ValueProto.ValueType.Enum;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

public class SpecServiceTest {

  @Mock
  private FeatureSetRepository featureSetRepository;

  @Mock
  private StoreRepository storeRepository;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private SpecService specService;
  private List<FeatureSet> featureSets;
  private List<Store> stores;
  private Source defaultSource;

  @Before
  public void setUp() {
    initMocks(this);
    defaultSource =
        new Source(
            SourceType.KAFKA,
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers("kafka:9092")
                .setTopic("my-topic")
                .build(),
            true);

    FeatureSet featureSet1v1 = newDummyFeatureSet("f1", 1);
    FeatureSet featureSet1v2 = newDummyFeatureSet("f1", 2);
    FeatureSet featureSet1v3 = newDummyFeatureSet("f1", 3);
    FeatureSet featureSet2v1 = newDummyFeatureSet("f2", 1);

    Field f3f1 = new Field("f3", "f3f1", Enum.INT64);
    Field f3f2 = new Field("f3", "f3f2", Enum.INT64);
    Field f3e1 = new Field("f3", "f3e1", Enum.STRING);
    FeatureSet featureSet3v1 = new FeatureSet(
        "f3", 1, 100L, Arrays.asList(f3e1), Arrays.asList(f3f2, f3f1), defaultSource);

    featureSets = Arrays
        .asList(featureSet1v1, featureSet1v2, featureSet1v3, featureSet2v1, featureSet3v1);
    when(featureSetRepository.findAll()).thenReturn(featureSets);
    when(featureSetRepository.findAllByOrderByNameAscVersionAsc()).thenReturn(featureSets);
    when(featureSetRepository.findByName("f1")).thenReturn(featureSets.subList(0, 3));
    when(featureSetRepository.findByName("f3")).thenReturn(featureSets.subList(4, 5));
    when(featureSetRepository.findFirstFeatureSetByNameOrderByVersionDesc("f1"))
        .thenReturn(featureSet1v3);
    when(featureSetRepository.findByNameWithWildcardOrderByNameAscVersionAsc("f1"))
        .thenReturn(featureSets.subList(0, 3));
    when(featureSetRepository.findByName("asd")).thenReturn(Lists.newArrayList());
    when(featureSetRepository.findByNameWithWildcardOrderByNameAscVersionAsc("f%"))
        .thenReturn(featureSets);

    Store store1 = newDummyStore("SERVING");
    Store store2 = newDummyStore("WAREHOUSE");
    stores = Arrays.asList(store1, store2);
    when(storeRepository.findAll()).thenReturn(stores);
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(store1));
    when(storeRepository.findById("NOTFOUND")).thenReturn(Optional.empty());

    specService = new SpecService(featureSetRepository, storeRepository, defaultSource);
  }

  @Test
  public void shouldGetAllFeatureSetsIfNoFilterProvided() throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(Filter.newBuilder().setFeatureSetName("").build());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet featureSet : featureSets) {
      FeatureSetSpec toProto = featureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingNameIfNoVersionProvided()
      throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(Filter.newBuilder().setFeatureSetName("f1").build());
    List<FeatureSet> expectedFeatureSets =
        featureSets.stream().filter(fs -> fs.getName().equals("f1")).collect(Collectors.toList());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingNameWithWildcardSearch()
      throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(Filter.newBuilder().setFeatureSetName("f*").build());
    List<FeatureSet> expectedFeatureSets =
        featureSets.stream()
            .filter(fs -> fs.getName().startsWith("f"))
            .collect(Collectors.toList());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingVersionIfNoComparator()
      throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion("1").build());
    List<FeatureSet> expectedFeatureSets =
        featureSets.stream()
            .filter(fs -> fs.getName().equals("f1"))
            .filter(fs -> fs.getVersion() == 1)
            .collect(Collectors.toList());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsGivenVersionWithComparator()
      throws InvalidProtocolBufferException {
    ListFeatureSetsResponse actual =
        specService.listFeatureSets(
            Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion(">1").build());
    List<FeatureSet> expectedFeatureSets =
        featureSets.stream()
            .filter(fs -> fs.getName().equals("f1"))
            .filter(fs -> fs.getVersion() > 1)
            .collect(Collectors.toList());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    ListFeatureSetsResponse expected =
        ListFeatureSetsResponse.newBuilder().addAllFeatureSets(list).build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetLatestFeatureSetGivenMissingVersionFilter()
      throws InvalidProtocolBufferException {
    GetFeatureSetResponse actual =
        specService.getFeatureSet(GetFeatureSetRequest.newBuilder().setName("f1").build());
    FeatureSet expected = featureSets.get(2);
    assertThat(actual.getFeatureSet(), equalTo(expected.toProto()));
  }

  @Test
  public void shouldGetSpecificFeatureSetGivenSpecificVersionFilter()
      throws InvalidProtocolBufferException {
    when(featureSetRepository.findFeatureSetByNameAndVersion("f1", 2))
        .thenReturn(featureSets.get(1));
    GetFeatureSetResponse actual =
        specService.getFeatureSet(
            GetFeatureSetRequest.newBuilder().setName("f1").setVersion(2).build());
    FeatureSet expected = featureSets.get(1);
    assertThat(actual.getFeatureSet(), equalTo(expected.toProto()));
  }

  @Test
  public void shouldThrowExceptionGivenMissingFeatureSetName()
      throws InvalidProtocolBufferException {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("INVALID_ARGUMENT: No feature set name provided");
    specService.getFeatureSet(GetFeatureSetRequest.newBuilder().setVersion(2).build());
  }

  @Test
  public void shouldThrowExceptionGivenMissingFeatureSet() throws InvalidProtocolBufferException {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage(
        "NOT_FOUND: Feature set with name \"f1000\" and version \"2\" could not be found.");
    specService.getFeatureSet(
        GetFeatureSetRequest.newBuilder().setName("f1000").setVersion(2).build());
  }

  @Test
  public void shouldThrowRetrievalExceptionGivenInvalidFeatureSetVersionComparator()
      throws InvalidProtocolBufferException {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Invalid comparator '=<' provided.");
    specService.listFeatureSets(
        Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion("=<1").build());
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
  public void applyFeatureSetShouldReturnFeatureSetWithLatestVersionIfFeatureSetHasNotChanged()
      throws InvalidProtocolBufferException {
    FeatureSetSpec incomingFeatureSet =
        featureSets.get(2).toProto().toBuilder().clearVersion().build();
    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(incomingFeatureSet);

    verify(featureSetRepository, times(0)).save(ArgumentMatchers.any(FeatureSet.class));
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.NO_CHANGE));
    assertThat(applyFeatureSetResponse.getFeatureSet(), equalTo(featureSets.get(2).toProto()));
  }

  @Test
  public void applyFeatureSetShouldApplyFeatureSetWithInitVersionIfNotExists()
      throws InvalidProtocolBufferException {
    when(featureSetRepository.findByName("f2")).thenReturn(Lists.newArrayList());
    FeatureSetSpec incomingFeatureSet =
        newDummyFeatureSet("f2", 1).toProto().toBuilder().clearVersion().build();
    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(incomingFeatureSet);
    verify(featureSetRepository).saveAndFlush(ArgumentMatchers.any(FeatureSet.class));
    FeatureSetSpec expected =
        incomingFeatureSet.toBuilder().setVersion(1).setSource(defaultSource.toProto()).build();
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.CREATED));
    assertThat(applyFeatureSetResponse.getFeatureSet(), equalTo(expected));
  }

  @Test
  public void applyFeatureSetShouldIncrementFeatureSetVersionIfAlreadyExists()
      throws InvalidProtocolBufferException {
    FeatureSetSpec incomingFeatureSet =
        featureSets
            .get(2)
            .toProto()
            .toBuilder()
            .clearVersion()
            .addFeatures(FeatureSpec.newBuilder().setName("feature2").setValueType(Enum.STRING))
            .build();
    FeatureSetSpec expected =
        incomingFeatureSet.toBuilder().setVersion(4).setSource(defaultSource.toProto()).build();
    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(incomingFeatureSet);
    verify(featureSetRepository).saveAndFlush(ArgumentMatchers.any(FeatureSet.class));
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.CREATED));
    assertThat(applyFeatureSetResponse.getFeatureSet(), equalTo(expected));
  }


  @Test
  public void applyFeatureSetShouldNotCreateFeatureSetIfFieldsUnordered()
      throws InvalidProtocolBufferException {

    Field f3f1 = new Field("f3", "f3f1", Enum.INT64);
    Field f3f2 = new Field("f3", "f3f2", Enum.INT64);
    Field f3e1 = new Field("f3", "f3e1", Enum.STRING);
    FeatureSetProto.FeatureSetSpec incomingFeatureSet = (new FeatureSet(
        "f3", 5, 100L, Arrays.asList(f3e1), Arrays.asList(f3f2, f3f1), defaultSource)).toProto();

    FeatureSetSpec expected = incomingFeatureSet;
    ApplyFeatureSetResponse applyFeatureSetResponse =
        specService.applyFeatureSet(incomingFeatureSet);
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.NO_CHANGE));
    assertThat(applyFeatureSetResponse.getFeatureSet().getMaxAge(), equalTo(expected.getMaxAge()));
    assertThat(applyFeatureSetResponse.getFeatureSet().getEntities(0),
        equalTo(expected.getEntities(0)));
    assertThat(applyFeatureSetResponse.getFeatureSet().getName(), equalTo(expected.getName()));
  }


  @Test
  public void shouldUpdateStoreIfConfigChanges() throws InvalidProtocolBufferException {
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(stores.get(0)));
    StoreProto.Store newStore =
        StoreProto.Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder())
            .addSubscriptions(Subscription.newBuilder().setName("a").setVersion(">1"))
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

  private FeatureSet newDummyFeatureSet(String name, int version) {
    Field feature = new Field(name, "feature", Enum.INT64);
    Field entity = new Field(name, "entity", Enum.STRING);
    return new FeatureSet(
        name, version, 100L, Arrays.asList(entity), Arrays.asList(feature), defaultSource);
  }

  private Store newDummyStore(String name) {
    // Add type to this method when we enable filtering by type
    Store store = new Store();
    store.setName(name);
    store.setType(StoreType.REDIS.toString());
    store.setSubscriptions("");
    store.setConfig(RedisConfig.newBuilder().setPort(6379).build().toByteArray());
    return store;
  }
}
