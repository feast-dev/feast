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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.common.it.DataGenerator;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesResponse;
import feast.proto.core.CoreServiceProto.ListProjectsRequest;
import feast.proto.core.CoreServiceProto.ListProjectsResponse;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.FeatureTableProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.Subscription;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.types.ValueProto;
import feast.serving.exception.SpecRetrievalException;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.CoreSpecService;
import feast.storage.api.retriever.FeatureSetRequest;
import java.util.HashMap;
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

  private ImmutableList<String> featureTableEntities;
  private ImmutableMap<String, ValueProto.ValueType.Enum> featureTable1Features;
  private ImmutableMap<String, ValueProto.ValueType.Enum> featureTable2Features;
  private FeatureTableSpec featureTable1Spec;
  private FeatureTableSpec featureTable2Spec;

  @Before
  public void setUp() {
    initMocks(this);

    this.store = Store.newBuilder().build();
    this.featureSetSpecs = new HashMap<>();

    this.setupFeatureSetAndStoreSubscription(
        "project",
        "fs1",
        List.of(
            FeatureSpec.newBuilder().setName("feature").build(),
            FeatureSpec.newBuilder().setName("feature2").build()));

    this.setupFeatureSetAndStoreSubscription(
        "default",
        "fs2",
        List.of(
            FeatureSpec.newBuilder().setName("feature3").build(),
            FeatureSpec.newBuilder().setName("feature4").build(),
            FeatureSpec.newBuilder().setName("feature5").build()));

    this.setupFeatureSetAndStoreSubscription(
        "default", "fs3", List.of(FeatureSpec.newBuilder().setName("feature4").build()));

    this.setupProject("default");
    this.featureTableEntities = ImmutableList.of("entity1");
    this.featureTable1Features =
        ImmutableMap.of(
            "trip_cost1", ValueProto.ValueType.Enum.INT64,
            "trip_distance1", ValueProto.ValueType.Enum.DOUBLE,
            "trip_empty1", ValueProto.ValueType.Enum.DOUBLE);
    this.featureTable2Features =
        ImmutableMap.of(
            "trip_cost2", ValueProto.ValueType.Enum.INT64,
            "trip_distance2", ValueProto.ValueType.Enum.DOUBLE,
            "trip_empty2", ValueProto.ValueType.Enum.DOUBLE);
    this.featureTable1Spec =
        DataGenerator.createFeatureTableSpec(
            "featuretable1",
            this.featureTableEntities,
            featureTable1Features,
            7200,
            ImmutableMap.of());
    this.featureTable2Spec =
        DataGenerator.createFeatureTableSpec(
            "featuretable2",
            this.featureTableEntities,
            featureTable2Features,
            7200,
            ImmutableMap.of());

    this.setupFeatureTableAndProject("default");

    when(this.coreService.registerStore(store)).thenReturn(store);
    cachedSpecService = new CachedSpecService(this.coreService, this.store);
  }

  private void setupProject(String project) {
    when(coreService.listProjects(ListProjectsRequest.newBuilder().build()))
        .thenReturn(ListProjectsResponse.newBuilder().addProjects(project).build());
  }

  private void setupFeatureTableAndProject(String project) {
    ImmutableMap<String, ValueProto.ValueType.Enum> featureTable1Features =
        this.featureTable1Features;

    FeatureTableProto.FeatureTable featureTable1 =
        FeatureTableProto.FeatureTable.newBuilder().setSpec(this.featureTable1Spec).build();
    FeatureTableProto.FeatureTable featureTable2 =
        FeatureTableProto.FeatureTable.newBuilder().setSpec(this.featureTable2Spec).build();

    when(coreService.listFeatureTables(
            ListFeatureTablesRequest.newBuilder()
                .setFilter(ListFeatureTablesRequest.Filter.newBuilder().setProject(project).build())
                .build()))
        .thenReturn(
            ListFeatureTablesResponse.newBuilder()
                .addTables(featureTable1)
                .addTables(featureTable2)
                .build());
  }

  private void setupFeatureSetAndStoreSubscription(
      String project, String name, List<FeatureSpec> featureSpecs) {
    FeatureSetSpec fsSpec =
        FeatureSetSpec.newBuilder()
            .setProject(project)
            .setName(name)
            .addAllFeatures(featureSpecs)
            .build();
    this.featureSetSpecs.put(String.format("%s", name), fsSpec);

    this.store =
        this.store
            .toBuilder()
            .addSubscriptions(Subscription.newBuilder().setProject(project).setName(name).build())
            .build();

    // collect the different versions the featureset with the given name
    FeatureSetProto.FeatureSet featureSet =
        FeatureSetProto.FeatureSet.newBuilder().setSpec(fsSpec).build();

    when(coreService.listFeatureSets(
            ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    ListFeatureSetsRequest.Filter.newBuilder()
                        .setProject(project)
                        .setFeatureSetName(name)
                        .build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addFeatureSets(featureSet).build());
  }

  @Test
  public void shouldRegisterStoreWithCore() {
    verify(coreService, times(1)).registerStore(cachedSpecService.getStore());
  }

  @Test
  public void shouldPopulateAndReturnStore() {
    cachedSpecService.populateCache();
    Store actual = cachedSpecService.getStore();
    assertThat(actual, equalTo(store));
  }

  @Test
  public void shouldPopulateAndReturnDifferentFeatureTables() {
    // test that CachedSpecService can retrieve fully qualified feature references.
    cachedSpecService.populateCache();
    FeatureReferenceV2 featureReference1 =
        FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable1")
            .setName("trip_cost1")
            .build();
    FeatureReferenceV2 featureReference2 =
        FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable1")
            .setName("trip_distance1")
            .build();
    FeatureReferenceV2 featureReference3 =
        FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretable2")
            .setName("trip_empty2")
            .build();

    assertThat(
        cachedSpecService.getFeatureTableSpec("default", featureReference1),
        equalTo(this.featureTable1Spec));
    assertThat(
        cachedSpecService.getFeatureTableSpec("default", featureReference2),
        equalTo(this.featureTable1Spec));
    assertThat(
        cachedSpecService.getFeatureTableSpec("default", featureReference3),
        equalTo(this.featureTable2Spec));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSets() {
    // test that CachedSpecService can retrieve fully qualified feature references.
    cachedSpecService.populateCache();
    FeatureReference fs1fr1 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature")
            .setFeatureSet("fs1")
            .build();
    FeatureReference fs1fr2 =
        FeatureReference.newBuilder()
            .setProject("project")
            .setName("feature2")
            .setFeatureSet("fs1")
            .build();

    assertThat(
        cachedSpecService.getFeatureSets(List.of(fs1fr1, fs1fr2)),
        equalTo(
            List.of(
                FeatureSetRequest.newBuilder()
                    .addFeatureReference(fs1fr1)
                    .addFeatureReference(fs1fr2)
                    .setSpec(featureSetSpecs.get("fs1"))
                    .build())));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSetWithDefaultProjectIfProjectNotSupplied() {
    // test that CachedSpecService will use default project when project unspecified
    FeatureReference fs2fr3 =
        FeatureReference.newBuilder().setName("feature3").setFeatureSet("fs2").build();
    // check that this is true for references in where feature set is unspecified
    FeatureReference fs2fr5 = FeatureReference.newBuilder().setName("feature5").build();

    assertThat(
        cachedSpecService.getFeatureSets(List.of(fs2fr3, fs2fr5)),
        equalTo(
            List.of(
                FeatureSetRequest.newBuilder()
                    .addFeatureReference(fs2fr3)
                    .addFeatureReference(fs2fr5)
                    .setSpec(featureSetSpecs.get("fs2"))
                    .build())));
  }

  @Test
  public void shouldPopulateAndReturnClosestFeatureSetIfFeatureSetNotSupplied() {
    // test that CachedSpecService will try to match a featureset without a featureset name in
    // reference
    FeatureReference fs1fr1 =
        FeatureReference.newBuilder().setProject("project").setName("feature").build();

    // check that this is true for reference in which project is unspecified
    FeatureReference fs2fr3 = FeatureReference.newBuilder().setName("feature3").build();

    assertThat(
        cachedSpecService.getFeatureSets(List.of(fs1fr1, fs2fr3)),
        containsInAnyOrder(
            List.of(
                    FeatureSetRequest.newBuilder()
                        .addFeatureReference(fs1fr1)
                        .setSpec(featureSetSpecs.get("fs1"))
                        .build(),
                    FeatureSetRequest.newBuilder()
                        .addFeatureReference(fs2fr3)
                        .setSpec(featureSetSpecs.get("fs2"))
                        .build())
                .toArray()));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSetsGivenFeaturesFromDifferentFeatureSets() {
    cachedSpecService.populateCache();
    FeatureReference fs1fr1 =
        FeatureReference.newBuilder().setProject("project").setName("feature").build();

    FeatureReference fs2fr3 =
        FeatureReference.newBuilder().setProject("default").setName("feature3").build();

    assertThat(
        cachedSpecService.getFeatureSets(List.of(fs1fr1, fs2fr3)),
        containsInAnyOrder(
            List.of(
                    FeatureSetRequest.newBuilder()
                        .addFeatureReference(fs1fr1)
                        .setSpec(featureSetSpecs.get("fs1"))
                        .build(),
                    FeatureSetRequest.newBuilder()
                        .addFeatureReference(fs2fr3)
                        .setSpec(featureSetSpecs.get("fs2"))
                        .build())
                .toArray()));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSetGivenFeaturesFromSameFeatureSet() {
    cachedSpecService.populateCache();
    FeatureReference fr1 =
        FeatureReference.newBuilder().setProject("project").setName("feature").build();
    FeatureReference fr2 =
        FeatureReference.newBuilder().setProject("project").setName("feature2").build();

    assertThat(
        cachedSpecService.getFeatureSets(List.of(fr1, fr2)),
        equalTo(
            List.of(
                FeatureSetRequest.newBuilder()
                    .addFeatureReference(fr1)
                    .addFeatureReference(fr2)
                    .setSpec(featureSetSpecs.get("fs1"))
                    .build())));
  }

  @Test
  public void shouldThrowExceptionWhenMultipleFeatureSetMapToFeatureReference()
      throws SpecRetrievalException {
    // both fs2 and fs3 have the feature with the same name.
    // using a generic feature reference only specifying the feature's name
    // should cause a multiple feature sets to match and throw an error
    FeatureReference fs2fr4 = FeatureReference.newBuilder().setName("feature4").build();
    FeatureReference fs3fr4 = FeatureReference.newBuilder().setName("feature4").build();

    expectedException.expect(SpecRetrievalException.class);
    cachedSpecService.getFeatureSets(List.of(fs2fr4, fs3fr4));
  }
}
