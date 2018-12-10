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
import feast.serving.ServingAPIProto.RequestDetail;
import feast.serving.ServingAPIProto.RequestType;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.config.AppConfig;
import feast.serving.model.Pair;
import feast.serving.model.RequestDetailWithSpec;
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
    when(featureStorage.getCurrentFeatures(any(String.class), any(List.class), any(List.class)))
        .thenReturn(Collections.emptyList());
    when(featureStorageRegistry.get(any(String.class))).thenReturn(featureStorage);

    String featureId = "entity.none.feature_1";
    FeatureSpec featureSpec = FeatureSpec.newBuilder().setId(featureId).build();
    RequestDetail requestDetail =
        RequestDetail.newBuilder().setType(RequestType.LAST).setFeatureId(featureId).build();
    RequestDetailWithSpec requestDetailWithSpec =
        new RequestDetailWithSpec(requestDetail, featureSpec);
    dispatcher.dispatchFeatureRetrieval(
        entityName, entityIds, Collections.singletonList(requestDetailWithSpec), null);

    verifyZeroInteractions(executorService);
    verify(featureStorage)
        .getCurrentFeatures(entityName, entityIds, Collections.singletonList(featureSpec));
  }

  @Test
  public void shouldUseCurrentThreadIfRequestFromSameStorage() {
    String storageId1 = "REDIS1";

    FeatureStorage redis1 = mock(FeatureStorage.class);
    when(featureStorageRegistry.get(storageId1)).thenReturn(redis1);

    when(redis1.getCurrentFeatures(any(String.class), any(List.class), any(List.class)))
        .thenReturn(Collections.emptyList());

    String entityName = "entity";
    String featureId1 = "entity.none.feature_1";
    int limit1 = 5;
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId1)))
            .build();
    RequestDetail requestDetail1 =
        RequestDetail.newBuilder()
            .setType(RequestType.LIST)
            .setLimit(limit1)
            .setFeatureId(featureId1)
            .build();

    String featureId2 = "entity.none.feature_2";
    int limit2 = 1;
    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId1)))
            .build();
    RequestDetail requestDetail2 =
        RequestDetail.newBuilder()
            .setType(RequestType.LIST)
            .setLimit(limit2)
            .setFeatureId(featureId2)
            .build();

    RequestDetailWithSpec requestDetailWithSpec1 =
        new RequestDetailWithSpec(requestDetail1, featureSpec1);
    RequestDetailWithSpec requestDetailWithSpec2 =
        new RequestDetailWithSpec(requestDetail2, featureSpec2);

    TimestampRange tsRange =
        TimestampRange.newBuilder()
            .setStart(Timestamps.fromSeconds(0))
            .setEnd(Timestamps.fromSeconds(10))
            .build();
    dispatcher.dispatchFeatureRetrieval(
        entityName,
        entityIds,
        Arrays.asList(requestDetailWithSpec1, requestDetailWithSpec2),
        tsRange);

    verify(redis1)
        .getNLatestFeaturesWithinTimestampRange(
            entityName,
            entityIds,
            Arrays.asList(new Pair<>(featureSpec1, limit1), new Pair<>(featureSpec2, limit2)),
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

    when(redis1.getCurrentFeatures(any(String.class), any(List.class), any(List.class)))
        .thenReturn(Collections.emptyList());
    when(redis2.getCurrentFeatures(any(String.class), any(List.class), any(List.class)))
        .thenReturn(Collections.emptyList());

    String entityName = "entity";
    String featureId1 = "entity.none.feature_1";
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId1)))
            .build();
    RequestDetail requestDetail1 =
        RequestDetail.newBuilder().setType(RequestType.LAST).setFeatureId(featureId1).build();

    String featureId2 = "entity.none.feature_2";
    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId2)))
            .build();
    RequestDetail requestDetail2 =
        RequestDetail.newBuilder().setType(RequestType.LAST).setFeatureId(featureId2).build();

    RequestDetailWithSpec requestDetailWithSpec1 =
        new RequestDetailWithSpec(requestDetail1, featureSpec1);
    RequestDetailWithSpec requestDetailWithSpec2 =
        new RequestDetailWithSpec(requestDetail2, featureSpec2);

    dispatcher.dispatchFeatureRetrieval(
        entityName, entityIds, Arrays.asList(requestDetailWithSpec1, requestDetailWithSpec2), null);

    verify(redis1)
        .getCurrentFeatures(entityName, entityIds, Collections.singletonList(featureSpec1));
    verify(redis2)
        .getCurrentFeatures(entityName, entityIds, Collections.singletonList(featureSpec2));
    verify(executorService, atLeast(2)).submit(any(Callable.class));
  }

  @Test
  public void shouldUseExecutorServiceIfMultipleRequestType() {
    String storageId1 = "REDIS1";
    String storageId2 = "REDIS2";

    FeatureStorage redis1 = mock(FeatureStorage.class);
    FeatureStorage redis2 = mock(FeatureStorage.class);
    when(featureStorageRegistry.get(storageId1)).thenReturn(redis1);
    when(featureStorageRegistry.get(storageId2)).thenReturn(redis2);

    when(redis1.getCurrentFeatures(any(String.class), any(List.class), any(List.class)))
        .thenReturn(Collections.emptyList());
    when(redis2.getCurrentFeatures(any(String.class), any(List.class), any(List.class)))
        .thenReturn(Collections.emptyList());

    String entityName = "entity";
    String featureId1 = "entity.none.feature_1";
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId1)))
            .build();
    RequestDetail requestDetail1 =
        RequestDetail.newBuilder().setType(RequestType.LAST).setFeatureId(featureId1).build();

    String featureId2 = "entity.none.feature_2";
    int limit = 5;
    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId(featureId1)
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId(storageId2)))
            .build();
    RequestDetail requestDetail2 =
        RequestDetail.newBuilder()
            .setType(RequestType.LIST)
            .setLimit(limit)
            .setFeatureId(featureId2)
            .build();

    RequestDetailWithSpec requestDetailWithSpec1 =
        new RequestDetailWithSpec(requestDetail1, featureSpec1);
    RequestDetailWithSpec requestDetailWithSpec2 =
        new RequestDetailWithSpec(requestDetail2, featureSpec2);

    TimestampRange tsRange =
        TimestampRange.newBuilder()
            .setStart(Timestamps.fromSeconds(0))
            .setEnd(Timestamps.fromSeconds(10))
            .build();
    dispatcher.dispatchFeatureRetrieval(
        entityName,
        entityIds,
        Arrays.asList(requestDetailWithSpec1, requestDetailWithSpec2),
        tsRange);

    verify(redis1)
        .getCurrentFeatures(entityName, entityIds, Collections.singletonList(featureSpec1));
    verify(redis2)
        .getNLatestFeaturesWithinTimestampRange(
            entityName, entityIds, Collections.singletonList(new Pair<>(featureSpec2, limit)), tsRange);
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
