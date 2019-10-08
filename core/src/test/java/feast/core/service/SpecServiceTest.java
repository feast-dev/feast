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
import feast.core.CoreServiceProto.GetFeatureSetsRequest.Filter;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
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

  @Mock
  private FeatureStreamService featureStreamService;

  @Mock
  private JobCoordinatorService jobCoordinatorService;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private SpecService specService;
  private List<FeatureSet> featureSets;
  private List<Store> stores;

  @Before
  public void setUp() {
    initMocks(this);
    FeatureSet featureSet1v1 = newDummyFeatureSet("f1", 1);
    FeatureSet featureSet1v2 = newDummyFeatureSet("f1", 2);
    FeatureSet featureSet1v3 = newDummyFeatureSet("f1", 3);
    FeatureSet featureSet2v1 = newDummyFeatureSet("f2", 1);
    featureSets = Arrays.asList(featureSet1v1, featureSet1v2, featureSet1v3, featureSet2v1);
    when(featureSetRepository.findAll())
        .thenReturn(featureSets);
    when(featureSetRepository.findByName("f1"))
        .thenReturn(featureSets.subList(0, 3));
    when(featureSetRepository.findByNameRegex("f1"))
        .thenReturn(featureSets.subList(0, 3));
    when(featureSetRepository.findByName("asd"))
        .thenReturn(Lists.newArrayList());
    when(featureSetRepository.findByNameRegex("asd"))
        .thenReturn(Lists.newArrayList());

    Store store1 = newDummyStore("SERVING");
    Store store2 = newDummyStore("WAREHOUSE");
    stores = Arrays.asList(store1, store2);
    when(storeRepository.findAll()).thenReturn(stores);
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(store1));
    when(storeRepository.findById("NOTFOUND")).thenReturn(Optional.empty());

    specService = new SpecService(featureSetRepository, storeRepository, featureStreamService,
        jobCoordinatorService);
  }

  @Test
  public void shouldGetAllFeatureSetsIfNoFilterProvided() throws InvalidProtocolBufferException {
    GetFeatureSetsResponse actual = specService
        .getFeatureSets(Filter.newBuilder().setFeatureSetName("").build());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet featureSet : featureSets) {
      FeatureSetSpec toProto = featureSet.toProto();
      list.add(toProto);
    }
    GetFeatureSetsResponse expected = GetFeatureSetsResponse
        .newBuilder()
        .addAllFeatureSets(
            list)
        .build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingNameIfNoVersionProvided()
      throws InvalidProtocolBufferException {
    GetFeatureSetsResponse actual = specService
        .getFeatureSets(Filter.newBuilder().setFeatureSetName("f1").build());
    List<FeatureSet> expectedFeatureSets = featureSets.stream()
        .filter(fs -> fs.getName().equals("f1"))
        .collect(Collectors.toList());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    GetFeatureSetsResponse expected = GetFeatureSetsResponse
        .newBuilder()
        .addAllFeatureSets(
            list)
        .build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingVersionIfNoComparator()
      throws InvalidProtocolBufferException {
    GetFeatureSetsResponse actual = specService
        .getFeatureSets(
            Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion("1").build());
    List<FeatureSet> expectedFeatureSets = featureSets.stream()
        .filter(fs -> fs.getName().equals("f1"))
        .filter(fs -> fs.getVersion() == 1)
        .collect(Collectors.toList());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    GetFeatureSetsResponse expected = GetFeatureSetsResponse
        .newBuilder()
        .addAllFeatureSets(
            list)
        .build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsGivenVersionWithComparator()
      throws InvalidProtocolBufferException {
    GetFeatureSetsResponse actual = specService
        .getFeatureSets(
            Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion(">1").build());
    List<FeatureSet> expectedFeatureSets = featureSets.stream()
        .filter(fs -> fs.getName().equals("f1"))
        .filter(fs -> fs.getVersion() > 1)
        .collect(Collectors.toList());
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    GetFeatureSetsResponse expected = GetFeatureSetsResponse
        .newBuilder()
        .addAllFeatureSets(
            list)
        .build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetLatestFeatureSetGivenLatestVersionFilter()
      throws InvalidProtocolBufferException {
    GetFeatureSetsResponse actual = specService
        .getFeatureSets(
            Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion("latest").build());
    List<FeatureSet> expectedFeatureSets = featureSets.subList(2,3);
    List<FeatureSetSpec> list = new ArrayList<>();
    for (FeatureSet expectedFeatureSet : expectedFeatureSets) {
      FeatureSetSpec toProto = expectedFeatureSet.toProto();
      list.add(toProto);
    }
    GetFeatureSetsResponse expected = GetFeatureSetsResponse
        .newBuilder()
        .addAllFeatureSets(
            list)
        .build();
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionGivenInvalidFeatureSetVersionComparator()
      throws InvalidProtocolBufferException {
    expectedException.expect(StatusRuntimeException.class);
    expectedException.expectMessage("Invalid comparator '=<' provided.");
    specService.getFeatureSets(
        Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion("=<1").build());
  }

  @Test
  public void shouldReturnAllStoresIfNoNameProvided() throws InvalidProtocolBufferException {
    GetStoresResponse actual = specService
        .getStores(GetStoresRequest.Filter.newBuilder().build());
    GetStoresResponse.Builder expected = GetStoresResponse.newBuilder();
    for (Store expectedStore : stores) {
      expected.addStore(expectedStore.toProto());
    }
    assertThat(actual, equalTo(expected.build()));
  }

  @Test
  public void shouldReturnStoreWithName() throws InvalidProtocolBufferException {
    GetStoresResponse actual = specService
        .getStores(GetStoresRequest.Filter.newBuilder().setName("SERVING").build());
    List<Store> expectedStores = stores.stream().filter(s -> s.getName().equals("SERVING"))
        .collect(Collectors.toList());
    GetStoresResponse.Builder expected = GetStoresResponse.newBuilder();
    for (Store expectedStore : expectedStores) {
      expected.addStore(expectedStore.toProto());
    }
    assertThat(actual, equalTo(expected.build()));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfNoStoresFoundWithName() {
    expectedException.expect(RetrievalException.class);
    expectedException.expectMessage("Store with name 'NOTFOUND' not found");
    specService
        .getStores(GetStoresRequest.Filter.newBuilder().setName("NOTFOUND").build());
  }

  @Test
  public void applyFeatureSetShouldReturnFeatureSetWithLatestVersionIfFeatureSetHasNotChanged()
      throws InvalidProtocolBufferException {
    FeatureSetSpec incomingFeatureSet = featureSets.get(2)
        .toProto()
        .toBuilder()
        .clearVersion()
        .build();
    ApplyFeatureSetResponse applyFeatureSetResponse = specService
        .applyFeatureSet(incomingFeatureSet);

    verify(featureSetRepository, times(0)).save(ArgumentMatchers.any(FeatureSet.class));
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.NO_CHANGE));
    assertThat(applyFeatureSetResponse.getFeatureSet(), equalTo(featureSets.get(2).toProto()));
  }

  @Test
  public void applyFeatureSetShouldApplyFeatureSetWithInitVersionIfNotExists()
      throws InvalidProtocolBufferException {
    when(featureSetRepository.findByName("f2")).thenReturn(Lists.newArrayList());
    Source updatedSource = new Source(SourceType.KAFKA,
        KafkaSourceConfig.newBuilder().setBootstrapServers("kafka:9092")
            .setTopic("feast-f2-features").build().toByteArray());
    when(featureStreamService.setUpSource(ArgumentMatchers.any(FeatureSet.class)))
        .thenReturn(updatedSource);
    FeatureSetSpec incomingFeatureSet = newDummyFeatureSet("f2", 1)
        .toProto()
        .toBuilder()
        .clearVersion()
        .build();
    ApplyFeatureSetResponse applyFeatureSetResponse = specService
        .applyFeatureSet(incomingFeatureSet);
    verify(featureSetRepository).saveAndFlush(ArgumentMatchers.any(FeatureSet.class));
    FeatureSetSpec expected = incomingFeatureSet.toBuilder()
        .setVersion(1)
        .setSource(updatedSource.toProto())
        .build();
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.CREATED));
    assertThat(applyFeatureSetResponse.getFeatureSet(), equalTo(expected));
  }

  @Test
  public void applyFeatureSetShouldIncrementFeatureSetVersionIfAlreadyExists()
      throws InvalidProtocolBufferException {
    Source updatedSource = new Source(SourceType.KAFKA,
        KafkaSourceConfig.newBuilder().setBootstrapServers("kafka:9092")
            .setTopic("feast-f1-features").build().toByteArray());
    when(featureStreamService.setUpSource(ArgumentMatchers.any(FeatureSet.class)))
        .thenReturn(updatedSource);
    FeatureSetSpec incomingFeatureSet = featureSets.get(2).toProto().toBuilder()
        .clearVersion()
        .addFeatures(FeatureSpec.newBuilder().setName("feature2").setValueType(Enum.STRING))
        .build();
    FeatureSetSpec expected = incomingFeatureSet.toBuilder()
        .setVersion(4)
        .setSource(updatedSource.toProto())
        .build();
    ApplyFeatureSetResponse applyFeatureSetResponse = specService
        .applyFeatureSet(incomingFeatureSet);
    verify(featureSetRepository).saveAndFlush(ArgumentMatchers.any(FeatureSet.class));
    assertThat(applyFeatureSetResponse.getStatus(), equalTo(Status.CREATED));
    assertThat(applyFeatureSetResponse.getFeatureSet(), equalTo(expected));
  }

  @Test
  public void shouldUpdateStoreIfConfigChanges() throws InvalidProtocolBufferException {
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(stores.get(0)));
    StoreProto.Store newStore = StoreProto.Store.newBuilder()
        .setName("SERVING")
        .setType(StoreType.REDIS)
        .setRedisConfig(RedisConfig.newBuilder())
        .addSubscriptions(Subscription.newBuilder().setName("a").setVersion(">1"))
        .build();
    UpdateStoreResponse actual = specService
        .updateStore(UpdateStoreRequest.newBuilder().setStore(newStore).build());
    UpdateStoreResponse expected = UpdateStoreResponse.newBuilder()
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
    UpdateStoreResponse actual = specService
        .updateStore(UpdateStoreRequest.newBuilder().setStore(stores.get(0).toProto()).build());
    UpdateStoreResponse expected = UpdateStoreResponse.newBuilder()
        .setStore(stores.get(0).toProto())
        .setStatus(UpdateStoreResponse.Status.NO_CHANGE)
        .build();
    verify(storeRepository, times(0)).save(ArgumentMatchers.any());
    assertThat(actual, equalTo(expected));
  }

  private FeatureSet newDummyFeatureSet(String name, int version) {
    KafkaSourceConfig kafkaFeatureSourceOptions =
        KafkaSourceConfig.newBuilder()
            .setBootstrapServers("kafka:9092")
            .build();
    Field feature = new Field(name, "feature", Enum.INT64);
    Field entity = new Field(name, "entity", Enum.STRING);
    return new FeatureSet(name, version, Arrays.asList(entity), Arrays.asList(feature),
        new Source(
            SourceType.KAFKA, kafkaFeatureSourceOptions.toByteArray()));
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

