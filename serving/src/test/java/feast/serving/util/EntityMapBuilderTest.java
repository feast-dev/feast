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

package feast.serving.util;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.Entity;
import feast.serving.model.FeatureValue;
import feast.types.ValueProto.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class EntityMapBuilderTest {
  @Test
  public void toEntityMap_shouldNotReturnNull() {
    EntityMapBuilder builder = new EntityMapBuilder();
    assertNotNull(builder.toEntityMap());
  }

  @Test
  public void addFeatureValueList_shouldWorkForOneEntityIdAndOneFeatureId() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(1);
    List<String> featureIds = createFeatureIds(1);
    Timestamp timestamp = Timestamps.fromSeconds(0);
    List<FeatureValue> featureValueList = createFeatureValues(entityIds, featureIds, timestamp);

    builder.addFeatureValueList(featureValueList);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, timestamp);
  }

  @Test
  public void addFeatureValueList_shouldWorkForSeveralEntityIdAndOneFeatureId() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(10);
    List<String> featureIds = createFeatureIds(1);
    Timestamp timestamp = Timestamps.fromSeconds(0);
    List<FeatureValue> featureValueList = createFeatureValues(entityIds, featureIds, timestamp);

    builder.addFeatureValueList(featureValueList);

    Map<String, Entity> result = builder.toEntityMap();
    validate(result, entityIds, featureIds, timestamp);
  }

  @Test
  public void addFeatureValueList_shouldWorkForSeveralEntityIdAndSeveralFeatureId() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(10);
    List<String> featureIds = createFeatureIds(10);
    Timestamp timestamp = Timestamps.fromSeconds(0);
    List<FeatureValue> featureValueList = createFeatureValues(entityIds, featureIds, timestamp);

    builder.addFeatureValueList(featureValueList);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, timestamp);
  }

  @Test
  public void addFeatureValueList_shouldWorkForMultipleCallOfDifferentEntityId() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(20);
    List<String> featureIds = createFeatureIds(20);
    Timestamp timestamp = Timestamps.fromSeconds(0);
    List<FeatureValue> featureValueList1 =
        createFeatureValues(entityIds.subList(0, 10), featureIds, timestamp);
    List<FeatureValue> featureValueList2 =
        createFeatureValues(entityIds.subList(10, 20), featureIds, timestamp);

    builder.addFeatureValueList(featureValueList1);
    builder.addFeatureValueList(featureValueList2);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, timestamp);
  }

  @Test
  public void addFeatureValueList_shouldWorkForMultipleCallOfDifferentFeature() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(20);
    List<String> featureIds = createFeatureIds(20);
    Timestamp timestamp = Timestamps.fromSeconds(0);
    List<FeatureValue> featureValueList1 =
        createFeatureValues(entityIds, featureIds.subList(0, 10), timestamp);
    List<FeatureValue> featureValueList2 =
        createFeatureValues(entityIds, featureIds.subList(10, 20), timestamp);

    builder.addFeatureValueList(featureValueList1);
    builder.addFeatureValueList(featureValueList2);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, timestamp);
  }

  @Test
  public void shouldBeThreadSafe() throws Exception {
    int nbThread = 16;
    int featurePerThread = 10;
    ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nbThread));
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean running = new AtomicBoolean();

    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(20);
    List<String> featureIds = createFeatureIds(featurePerThread * nbThread);
    Timestamp timestamp = Timestamps.fromSeconds(0);

    List<List<FeatureValue>> featureValueList = new ArrayList<>();
    for (int i = 0; i < nbThread; i++) {
      featureValueList.add(
          createFeatureValues(
              entityIds,
              featureIds.subList(i * featurePerThread, (i + 1) * featurePerThread),
              timestamp));
    }

    List<ListenableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < nbThread; i++) {
      final int j = i;
      futures.add(
          service.submit(
              () -> {
                try {
                  latch.await();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                running.set(true);
                builder.addFeatureValueList(featureValueList.get(j));
                running.set(false);
                return null;
              }));
    }

    ListenableFuture<List<Void>> all = Futures.allAsList(futures);
    latch.countDown();

    all.get();

    validate(builder.toEntityMap(), entityIds, featureIds, timestamp);
  }

  private void validate(
      Map<String, Entity> result,
      List<String> entityIds,
      List<String> featureIds,
      Timestamp timestamp) {
    assertThat(result.size(), equalTo(entityIds.size()));
    for (String entityId : entityIds) {
      Entity entity = result.get(entityId);
      assertNotNull(entity);
      Map<String, ServingAPIProto.FeatureValue> featureValueMap = entity.getFeaturesMap();
      assertNotNull(featureValueMap);
      assertThat(featureValueMap.size(), equalTo(featureIds.size()));
      for (String featureId : featureIds) {
        ServingAPIProto.FeatureValue featureValue = featureValueMap.get(featureId);

        assertThat(featureValue.getTimestamp(), equalTo(timestamp));
        assertThat(timestamp.getSeconds(), equalTo(featureValue.getValue().getInt64Val()));
      }
    }
  }

  private List<String> createFeatureIds(int count) {
    List<String> featureIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      featureIds.add("entity.feature_" + i);
    }
    return featureIds;
  }

  private List<String> createEntityIds(int count) {
    List<String> entityIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      entityIds.add("entity_" + i);
    }
    return entityIds;
  }

  private List<FeatureValue> createFeatureValues(
      List<String> entityIds, List<String> featureIds, Timestamp timestamp) {
    List<FeatureValue> featureValues = new ArrayList<>();
    for (String entityId : entityIds) {
      for (String featureId : featureIds) {
        featureValues.add(
            new FeatureValue(
                featureId,
                entityId,
                Value.newBuilder().setInt64Val(timestamp.getSeconds()).build(),
                timestamp));
      }
    }
    return featureValues;
  }
}
