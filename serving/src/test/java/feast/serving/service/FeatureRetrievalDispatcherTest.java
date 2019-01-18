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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.config.AppConfig;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FeatureRetrievalDispatcherTest {
  @Mock FeatureStorageRegistry featureStorageRegistry;
  private FeatureRetrievalDispatcher dispatcher;
  private List<String> entityIds;
  private ListeningExecutorService executorService;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    entityIds = createEntityIds(10);

    AppConfig appConfig = AppConfig.builder().timeout(1).build();
    executorService = spy(MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor()));
    dispatcher =
        new FeatureRetrievalDispatcher(
            featureStorageRegistry, executorService, appConfig, GlobalTracer.get());
  }

  @Test
  public void shouldUseCurrentThreadIfRequestIsSmallEnough() {
    String entityName = "entity";
    FeatureStorage featureStorage = mock(FeatureStorage.class);
    when(featureStorage.getFeature(
            any(String.class), any(List.class), any(List.class), any(TimestampRange.class)))
        .thenReturn(Collections.emptyList());
    when(featureStorageRegistry.get(any(String.class))).thenReturn(featureStorage);

    String featureId = "entity.none.feature_1";
    FeatureSpec featureSpec = FeatureSpec.newBuilder().setId(featureId).build();
    dispatcher.dispatchFeatureRetrieval(
        entityName, entityIds, Collections.singletonList(featureSpec), null);

    verifyZeroInteractions(executorService);
    verify(featureStorage)
        .getFeature(entityName, entityIds, Collections.singletonList(featureSpec), null);
  }

  @Test
  public void shouldUseCurrentThreadIfRequestFromSameStorage() {
    String storageId1 = "REDIS1";

    FeatureStorage redis1 = mock(FeatureStorage.class);
    when(featureStorageRegistry.get(storageId1)).thenReturn(redis1);

    when(redis1.getFeature(
            any(String.class), any(List.class), any(List.class), any(TimestampRange.class)))
        .thenReturn(Collections.emptyList());

    String entityName = "entity";
    String featureId1 = "entity.none.feature_1";
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId1)))
            .build();

    String featureId2 = "entity.none.feature_2";
    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId(featureId2)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId1)))
            .build();

    TimestampRange tsRange =
        TimestampRange.newBuilder()
            .setStart(Timestamps.fromSeconds(0))
            .setEnd(Timestamps.fromSeconds(10))
            .build();
    dispatcher.dispatchFeatureRetrieval(
        entityName,
        entityIds,
        Arrays.asList(featureSpec1, featureSpec2),
        tsRange);

    verify(redis1)
        .getFeature(
            entityName,
            entityIds,
            Arrays.asList(featureSpec1, featureSpec2),
            tsRange);
    verifyZeroInteractions(executorService);
  }

  @Test
  public void shouldUseExecutorServiceIfRequestFromMoreThanOneStorage() {
    String storageId1 = "REDIS1";
    String storageId2 = "REDIS2";

    FeatureStorage redis1 = mock(FeatureStorage.class);
    FeatureStorage redis2 = mock(FeatureStorage.class);
    when(featureStorageRegistry.get(storageId1)).thenReturn(redis1);
    when(featureStorageRegistry.get(storageId2)).thenReturn(redis2);

    when(redis1.getFeature(
            any(String.class), any(List.class), any(List.class), any(TimestampRange.class)))
        .thenReturn(Collections.emptyList());
    when(redis2.getFeature(
            any(String.class), any(List.class), any(List.class), any(TimestampRange.class)))
        .thenReturn(Collections.emptyList());

    String entityName = "entity";
    String featureId1 = "entity.none.feature_1";
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId1)))
            .build();

    String featureId2 = "entity.none.feature_2";
    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId(featureId2)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId2)))
            .build();

    dispatcher.dispatchFeatureRetrieval(
        entityName, entityIds, Arrays.asList(featureSpec1, featureSpec2), null);

    verify(redis1).getFeature(entityName, entityIds, Collections.singletonList(featureSpec1), null);
    verify(redis2).getFeature(entityName, entityIds, Collections.singletonList(featureSpec2), null);
    verify(executorService, atLeast(2)).submit(any(Callable.class));
  }

  private List<String> createEntityIds(int count) {
    List<String> entityIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      entityIds.add("entity_" + i);
    }
    return entityIds;
  }
}
