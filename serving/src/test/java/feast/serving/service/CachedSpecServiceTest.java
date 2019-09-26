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

package feast.serving.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.service.spec.CachedSpecService;
import feast.serving.service.spec.SpecService;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;


public class CachedSpecServiceTest {

  private static final String STORE_ID = "SERVING";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  SpecService coreService;

  private Map<String, FeatureSetSpec> featureSetSpecs;
  private Store store;
  private CachedSpecService cachedSpecService;

  @Before
  public void setUp() {
    initMocks(this);

    featureSetSpecs = new LinkedHashMap<>();
    featureSetSpecs.put("fs1:1", FeatureSetSpec.newBuilder().setName("fs1").setVersion(1).build());
    featureSetSpecs.put("fs2:1", FeatureSetSpec.newBuilder().setName("fs2").setVersion(1).build());
    featureSetSpecs.put("fs1:2", FeatureSetSpec.newBuilder().setName("fs1").setVersion(2).build());
    featureSetSpecs.put("fs1:3", FeatureSetSpec.newBuilder().setName("fs1").setVersion(3).build());

    store = Store.newBuilder().setName(STORE_ID)
        .addSubscriptions(Subscription.newBuilder().setName("fs1").setVersion(">0").build())
        .addSubscriptions(Subscription.newBuilder().setName("fs2").setVersion(">0").build())
        .build();

    when(coreService.getStoreDetails(STORE_ID)).thenReturn(store);
    when(coreService.getFeatureSetSpecs(store.getSubscriptionsList())).thenReturn(featureSetSpecs);
    cachedSpecService = new CachedSpecService(coreService, STORE_ID);
  }

  @Test
  public void shouldPopulateAndReturnStore() {
    cachedSpecService.populateCache();
    Store actual = cachedSpecService.getStoreDetails(STORE_ID);
    assertThat(actual, equalTo(store));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSets() {
    cachedSpecService.populateCache();
    Map<String, FeatureSetSpec> actual = cachedSpecService
        .getFeatureSetSpecs(store.getSubscriptionsList());
    assertThat(actual, equalTo(featureSetSpecs));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSetMatchingIdIfPresent() {
    cachedSpecService.populateCache();
    FeatureSetSpec featureSetSpec = cachedSpecService.getFeatureSetSpec("fs1:2");
    assertThat(featureSetSpec, equalTo(featureSetSpecs.get("fs1:2")));
  }

  @Test
  public void shouldThrowErrorIfFeatureSetMatchingIdNotPresent() {
    cachedSpecService.populateCache();
    expectedException.expect(FeatureRetrievalException.class);
    cachedSpecService.getFeatureSetSpec("fs1:10");
  }

  @Test
  public void shouldTryToReloadCacheIfFeatureSetMatchingIdNotPresent() {
    cachedSpecService.populateCache();
    expectedException.expect(FeatureRetrievalException.class);
    cachedSpecService.getFeatureSetSpec("fs1:10");
    verify(cachedSpecService, Mockito.times(1)).populateCache();
  }
}
