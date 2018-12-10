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
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.Entity;
import feast.serving.ServingAPIProto.FeatureValueList;
import feast.serving.model.FeatureValue;
import feast.types.ValueProto.TimestampList;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueList;
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
    Timestamp start = Timestamps.fromSeconds(0);
    Timestamp end = Timestamps.fromSeconds(10);
    List<FeatureValue> featureValueList = createFeatureValues(entityIds, featureIds, start, end);

    builder.addFeatureValueList(featureValueList);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, start, end);
  }

  @Test
  public void addFeatureValueList_shouldWorkForSeveralEntityIdAndOneFeatureId() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(10);
    List<String> featureIds = createFeatureIds(1);
    Timestamp start = Timestamps.fromSeconds(0);
    Timestamp end = Timestamps.fromSeconds(10);
    List<FeatureValue> featureValueList = createFeatureValues(entityIds, featureIds, start, end);

    builder.addFeatureValueList(featureValueList);

    Map<String, Entity> result = builder.toEntityMap();
    validate(result, entityIds, featureIds, start, end);
  }

  @Test
  public void addFeatureValueList_shouldWorkForSeveralEntityIdAndSeveralFeatureId() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(10);
    List<String> featureIds = createFeatureIds(10);
    Timestamp start = Timestamps.fromSeconds(0);
    Timestamp end = Timestamps.fromSeconds(10);
    List<FeatureValue> featureValueList = createFeatureValues(entityIds, featureIds, start, end);

    builder.addFeatureValueList(featureValueList);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, start, end);
  }

  @Test
  public void addFeatureValueList_shouldWorkForMultipleCallOfDifferentEntityId() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(20);
    List<String> featureIds = createFeatureIds(20);
    Timestamp start = Timestamps.fromSeconds(0);
    Timestamp end = Timestamps.fromSeconds(10);
    List<FeatureValue> featureValueList1 =
        createFeatureValues(entityIds.subList(0, 10), featureIds, start, end);
    List<FeatureValue> featureValueList2 =
        createFeatureValues(entityIds.subList(10, 20), featureIds, start, end);

    builder.addFeatureValueList(featureValueList1);
    builder.addFeatureValueList(featureValueList2);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, start, end);
  }

  @Test
  public void addFeatureValueList_shouldWorkForMultipleCallOfDifferentFeature() {
    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(20);
    List<String> featureIds = createFeatureIds(20);
    Timestamp start = Timestamps.fromSeconds(0);
    Timestamp end = Timestamps.fromSeconds(10);
    List<FeatureValue> featureValueList1 =
        createFeatureValues(entityIds, featureIds.subList(0, 10), start, end);
    List<FeatureValue> featureValueList2 =
        createFeatureValues(entityIds, featureIds.subList(10, 20), start, end);

    builder.addFeatureValueList(featureValueList1);
    builder.addFeatureValueList(featureValueList2);

    Map<String, Entity> result = builder.toEntityMap();

    validate(result, entityIds, featureIds, start, end);
  }

  @Test
  public void shouldBeThreadSafe() throws Exception {
    int nbThread = 16;
    int featurePerThread = 10;
    ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(nbThread));
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean running = new AtomicBoolean();
    AtomicInteger overlaps = new AtomicInteger();

    EntityMapBuilder builder = new EntityMapBuilder();
    List<String> entityIds = createEntityIds(20);
    List<String> featureIds = createFeatureIds(featurePerThread * nbThread);
    Timestamp start = Timestamps.fromSeconds(0);
    Timestamp end = Timestamps.fromSeconds(10);

    List<List<FeatureValue>> featureValueList = new ArrayList<>();
    for (int i = 0; i < nbThread; i++) {
      featureValueList.add(
          createFeatureValues(
              entityIds,
              featureIds.subList(i * featurePerThread, (i + 1) * featurePerThread),
              start,
              end));
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
                if (running.get()) {
                  overlaps.incrementAndGet();
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

    validate(builder.toEntityMap(), entityIds, featureIds, start, end);
    assertThat(overlaps.get(), greaterThan(0));
  }

  private void validate(
      Map<String, Entity> result,
      List<String> entityIds,
      List<String> featureIds,
      Timestamp start,
      Timestamp end) {
    assertThat(result.size(), equalTo(entityIds.size()));
    for (String entityId : entityIds) {
      Entity entity = result.get(entityId);
      assertNotNull(entity);
      Map<String, FeatureValueList> featureValueListMap = entity.getFeaturesMap();
      assertNotNull(featureValueListMap);
      assertThat(featureValueListMap.size(), equalTo(featureIds.size()));
      for (String featureId : featureIds) {
        FeatureValueList featureValueList = featureValueListMap.get(featureId);
        Duration duration = Timestamps.between(start, end);
        int count = (int) duration.getSeconds();

        ValueList valueList = featureValueList.getValueList();
        TimestampList tsList = featureValueList.getTimestampList();
        assertThat(valueList.getInt64List().getValCount(), equalTo(count));
        assertThat(tsList.getValCount(), equalTo(count));

        // check ordering
        for (int i = 0; i < count; i++) {
          if (i == 0) {
            continue;
          }

          // compare that timestamp is ordered in descending order.
          assertThat(Timestamps.compare(tsList.getVal(i), tsList.getVal(i - 1)), lessThan(0));
        }
      }
    }
  }

  private List<String> createFeatureIds(int count) {
    List<String> featureIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      featureIds.add("entity.none.feature_" + i);
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
      List<String> entityIds, List<String> featureIds, Timestamp start, Timestamp end) {
    List<FeatureValue> featureValues = new ArrayList<>();
    for (String entityId : entityIds) {
      for (String featureId : featureIds) {
        for (Timestamp iter = start;
            Timestamps.compare(iter, end) < 0;
            iter = Timestamps.add(iter, Durations.fromSeconds(1))) {
          featureValues.add(
              new FeatureValue(
                  featureId,
                  entityId,
                  Value.newBuilder().setInt64Val(iter.getSeconds()).build(),
                  iter));
        }
      }
    }
    return featureValues;
  }
}
