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
package feast.serving.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.CoreSpecService;
import feast.storage.api.retriever.FeatureSetRequest;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class CachedSpecServiceTest {

  private Store store;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock CoreSpecService coreService;

  private Map<String, FeatureSetSpec> featureSetSpecs;
  private CachedSpecService cachedSpecService;

  @Before
  public void setUp() {
    initMocks(this);

    store =
        Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
            .addSubscriptions(
                Subscription.newBuilder()
                    .setProject("project")
                    .setName("fs1")
                    .setVersion("*")
                    .build())
            .addSubscriptions(
                Subscription.newBuilder()
                    .setProject("project")
                    .setName("fs2")
                    .setVersion("*")
                    .build())
            .build();

    when(coreService.updateStore(UpdateStoreRequest.newBuilder().setStore(store).build()))
        .thenReturn(UpdateStoreResponse.newBuilder().setStore(store).build());

    featureSetSpecs = new LinkedHashMap<>();
    featureSetSpecs.put(
        "fs1:1",
        FeatureSetSpec.newBuilder()
            .setProject("project")
            .setName("fs1")
            .setVersion(1)
            .addFeatures(FeatureSpec.newBuilder().setName("feature"))
            .build());
    featureSetSpecs.put(
        "fs1:2",
        FeatureSetSpec.newBuilder()
            .setProject("project")
            .setName("fs1")
            .setVersion(2)
            .addFeatures(FeatureSpec.newBuilder().setName("feature"))
            .addFeatures(FeatureSpec.newBuilder().setName("feature2"))
            .build());
    featureSetSpecs.put(
        "fs2:1",
        FeatureSetSpec.newBuilder()
            .setProject("project")
            .setName("fs2")
            .setVersion(1)
            .addFeatures(FeatureSpec.newBuilder().setName("feature3"))
            .build());

    List<FeatureSetProto.FeatureSet> fs1FeatureSets =
        Lists.newArrayList(
            FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpecs.get("fs1:1")).build(),
            FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpecs.get("fs1:2")).build());
    List<FeatureSetProto.FeatureSet> fs2FeatureSets =
        Lists.newArrayList(
            FeatureSetProto.FeatureSet.newBuilder().setSpec(featureSetSpecs.get("fs2:1")).build());
    when(coreService.listFeatureSets(
            ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    ListFeatureSetsRequest.Filter.newBuilder()
                        .setProject("project")
                        .setFeatureSetName("fs1")
                        .setFeatureSetVersion("*")
                        .build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addAllFeatureSets(fs1FeatureSets).build());
    when(coreService.listFeatureSets(
            ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    ListFeatureSetsRequest.Filter.newBuilder()
                        .setProject("project")
                        .setFeatureSetName("fs2")
                        .setFeatureSetVersion("*")
                        .build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addAllFeatureSets(fs2FeatureSets).build());

    cachedSpecService = new CachedSpecService(coreService, store);
  }

  @Test
  public void shouldPopulateAndReturnStore() {
    cachedSpecService.populateCache();
    Store actual = cachedSpecService.getStore();
    assertThat(actual, equalTo(store));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSets() {
    cachedSpecService.populateCache();
    FeatureReference frv1 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature")
            .setVersion(1)
            .build();
    FeatureReference frv2 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature")
            .setVersion(2)
            .build();

    assertThat(
        cachedSpecService.getFeatureSets(Collections.singletonList(frv1)),
        equalTo(
            Lists.newArrayList(
                FeatureSetRequest.newBuilder()
                    .addFeatureReference(frv1)
                    .setSpec(featureSetSpecs.get("fs1:1"))
                    .build())));
    assertThat(
        cachedSpecService.getFeatureSets(Collections.singletonList(frv2)),
        equalTo(
            Lists.newArrayList(
                FeatureSetRequest.newBuilder()
                    .addFeatureReference(frv2)
                    .setSpec(featureSetSpecs.get("fs1:2"))
                    .build())));
  }

  @Test
  public void shouldPopulateAndReturnLatestFeatureSetIfVersionsNotSupplied() {
    cachedSpecService.populateCache();
    FeatureReference frv1 =
        FeatureReference.newBuilder().setProject("project").setName("feature").build();

    assertThat(
        cachedSpecService.getFeatureSets(Collections.singletonList(frv1)),
        equalTo(
            Lists.newArrayList(
                FeatureSetRequest.newBuilder()
                    .addFeatureReference(frv1)
                    .setSpec(featureSetSpecs.get("fs1:2"))
                    .build())));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSetsGivenFeaturesFromDifferentFeatureSets() {
    cachedSpecService.populateCache();
    FeatureReference frv1 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature")
            .setVersion(1)
            .build();
    FeatureReference fr3 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature3")
            .setVersion(1)
            .build();

    assertThat(
        cachedSpecService.getFeatureSets(Lists.newArrayList(frv1, fr3)),
        containsInAnyOrder(
            Lists.newArrayList(
                    FeatureSetRequest.newBuilder()
                        .addFeatureReference(frv1)
                        .setSpec(featureSetSpecs.get("fs1:1"))
                        .build(),
                    FeatureSetRequest.newBuilder()
                        .addFeatureReference(fr3)
                        .setSpec(featureSetSpecs.get("fs2:1"))
                        .build())
                .toArray()));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSetGivenFeaturesFromSameFeatureSet() {
    cachedSpecService.populateCache();
    FeatureReference fr1 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature")
            .setVersion(2)
            .build();
    FeatureReference fr2 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature2")
            .setVersion(2)
            .build();

    assertThat(
        cachedSpecService.getFeatureSets(Lists.newArrayList(fr1, fr2)),
        equalTo(
            Lists.newArrayList(
                FeatureSetRequest.newBuilder()
                    .addFeatureReference(fr1)
                    .addFeatureReference(fr2)
                    .setSpec(featureSetSpecs.get("fs1:2"))
                    .build())));
  }
}
