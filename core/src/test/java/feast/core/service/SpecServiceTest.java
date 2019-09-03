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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import feast.core.CoreServiceProto.GetFeatureSetsRequest.Filter;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.Source.SourceType;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.StoreRepository;
import feast.core.exception.RetrievalException;
import feast.core.model.FeatureSet;
import feast.core.model.Source;
import feast.core.model.Store;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.springframework.transaction.annotation.Transactional;

public class SpecServiceTest {

  @Mock
  private FeatureSetRepository featureSetRepository;

  @Mock
  private StoreRepository storeRepository;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
    when(featureSetRepository.findByName("asd"))
        .thenReturn(Lists.newArrayList());

    Store store1 = newDummyStore("SERVING");
    Store store2 = newDummyStore("WAREHOUSE");
    stores = Arrays.asList(store1, store2);
    when(storeRepository.findAll()).thenReturn(stores);
    when(storeRepository.findById("SERVING")).thenReturn(Optional.of(store1));
    when(storeRepository.findById("NOTFOUND")).thenReturn(Optional.empty());
  }

  @Test
  public void shouldGetAllFeatureSetsIfNoFilterProvided() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    List<FeatureSet> actual = specService
        .getFeatureSets(Filter.newBuilder().setFeatureSetName("").build());
    assertThat(actual, equalTo(featureSets));
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingNameIfNoVersionProvided() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    List<FeatureSet> actual = specService
        .getFeatureSets(Filter.newBuilder().setFeatureSetName("f1").build());
    List<FeatureSet> expected = featureSets.stream().filter(fs -> fs.getName().equals("f1"))
        .collect(Collectors.toList());
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowErrorWhenNoFeatureSetsWithNameFound() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    Filter filter = Filter.newBuilder().setFeatureSetName("asd").build();
    expectedException.expect(RetrievalException.class);
    expectedException.expectMessage(
        String.format("Unable to find any featureSets matching the filter '%s'", filter));
    specService.getFeatureSets(filter);
  }

  @Test
  public void shouldGetAllFeatureSetsMatchingVersionIfNoComparator() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    List<FeatureSet> actual = specService
        .getFeatureSets(
            Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion("1").build());
    List<FeatureSet> expected = featureSets.stream()
        .filter(fs -> fs.getName().equals("f1"))
        .filter(fs -> fs.getVersion() == 1)
        .collect(Collectors.toList());
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldGetAllFeatureSetsGivenVersionWithComparator() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    List<FeatureSet> actual = specService
        .getFeatureSets(
            Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion(">1").build());
    List<FeatureSet> expected = featureSets.stream()
        .filter(fs -> fs.getName().equals("f1"))
        .filter(fs -> fs.getVersion() > 1)
        .collect(Collectors.toList());
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionGivenInvalidFeatureSetVersionComparator() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    expectedException.expect(RetrievalException.class);
    expectedException.expectMessage("Invalid comparator '=<' provided.");
    specService.getFeatureSets(
        Filter.newBuilder().setFeatureSetName("f1").setFeatureSetVersion("=<1").build());
  }

  @Test
  public void shouldReturnAllStoresIfNoNameProvided() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    List<Store> actual = specService.getStores(GetStoresRequest.Filter.newBuilder().build());
    assertThat(actual, equalTo(stores));
  }

  @Test
  public void shouldReturnStoreWithName() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    List<Store> actual = specService
        .getStores(GetStoresRequest.Filter.newBuilder().setName("SERVING").build());
    List<Store> expected = stores.stream().filter(s -> s.getName().equals("SERVING"))
        .collect(Collectors.toList());
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldThrowRetrievalExceptionIfNoStoresFoundWithName() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    expectedException.expect(RetrievalException.class);
    expectedException.expectMessage("Store with name 'NOTFOUND' not found");
    specService
        .getStores(GetStoresRequest.Filter.newBuilder().setName("NOTFOUND").build());
  }

  @Test
  public void applyFeatureSetShouldDoNothingIfFeatureSetHasNotChanged() {
    SpecService specService = new SpecService(featureSetRepository, storeRepository);
    FeatureSetSpec incomingFeatureSet = featureSets.get(2).toProto();
    incomingFeatureSet = incomingFeatureSet.toBuilder().setVersion(0).build();
    specService.applyFeatureSet(incomingFeatureSet);
    verify(featureSetRepository, times(0)).save(any(FeatureSet.class));
  }

  private FeatureSet newDummyFeatureSet(String name, int version) {
    Map<String, String> kafkaFeatureSourceOptions = Maps.newHashMap();
    kafkaFeatureSourceOptions.put("bootstrapServers", "kafka:9092");
    kafkaFeatureSourceOptions.put("topics", "my-featureset-topic");
    return new FeatureSet(name, version, Lists.newArrayList(), Lists.newArrayList(),
        new Source(
            SourceType.KAFKA, kafkaFeatureSourceOptions));
  }

  private Store newDummyStore(String name) {
    // Add type to this method when we enable filtering by type
    Store store = new Store();
    store.setName(name);
    return store;
  }
}

