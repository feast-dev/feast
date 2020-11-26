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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.common.it.DataGenerator;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesResponse;
import feast.proto.core.CoreServiceProto.ListProjectsRequest;
import feast.proto.core.CoreServiceProto.ListProjectsResponse;
import feast.proto.core.FeatureTableProto;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.core.StoreProto.Store;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.types.ValueProto;
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.CoreSpecService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class CachedSpecServiceTest {

  private Store store;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock CoreSpecService coreService;

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
}
