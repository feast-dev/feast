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

package feast.serving.testutil;

import feast.serving.service.BigTableFeatureStorage;
import feast.serving.service.RedisFeatureStorage;
import feast.serving.service.SpecStorage;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class FakeSpecStorage implements SpecStorage {

  Map<String, EntitySpec> entitySpecMap = new HashMap<>();
  Map<String, FeatureSpec> featureSpecMap = new HashMap<>();
  Map<String, StorageSpec> storageSpecMap = new HashMap<>();

  public FakeSpecStorage() {
    // populate with hardcoded value
    String bigTableId = "BIGTABLE1";
    String lastOpportunityId = "driver.last_opportunity";
    String lastOpportunityName = "last_opportunity";
    String dailyCompletedBookingId = "driver.total_completed_booking";
    String dailyCompletedBookingName = "total_completed_booking";
    DataStore bigTable = DataStore.newBuilder().setId(bigTableId).build();

    String redisId = "REDIS1";
    DataStore redis = DataStore.newBuilder().setId(redisId).build();

    EntitySpec driver = EntitySpec.newBuilder().setName("driver").build();

    entitySpecMap.put("driver", driver);

    FeatureSpec lastOpportunity =
        FeatureSpec.newBuilder()
            .setId(lastOpportunityId)
            .setName(lastOpportunityName)
            .setValueType(ValueType.Enum.INT64)
            .setDataStores(DataStores.newBuilder().setServing(redis).build())
            .build();

    FeatureSpec totalCompleted =
        FeatureSpec.newBuilder()
            .setId(dailyCompletedBookingId)
            .setName(dailyCompletedBookingName)
            .setValueType(ValueType.Enum.INT64)
            .setDataStores(DataStores.newBuilder().setServing(redis).build())
            .build();

    featureSpecMap.put(lastOpportunityId, lastOpportunity);
    featureSpecMap.put(dailyCompletedBookingId, totalCompleted);

    StorageSpec bigTableSpec =
        StorageSpec.newBuilder()
            .setId(bigTableId)
            .setType(RedisFeatureStorage.TYPE)
            .putOptions(BigTableFeatureStorage.OPT_BIGTABLE_PROJECT, "the-big-data-staging-007")
            .putOptions(BigTableFeatureStorage.OPT_BIGTABLE_INSTANCE, "ds-staging")
            .build();

    StorageSpec redisSpec =
        StorageSpec.newBuilder()
            .setId(redisId)
            .setType(RedisFeatureStorage.TYPE)
            .putOptions(RedisFeatureStorage.OPT_REDIS_HOST, "10.148.0.6")
            .putOptions(RedisFeatureStorage.OPT_REDIS_PORT, "6379")
            .build();

    storageSpecMap.put(bigTableId, bigTableSpec);
    storageSpecMap.put(redisId, redisSpec);
  }

  @Override
  public Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds) {
    return StreamSupport.stream(entityIds.spliterator(), false)
        .filter(entitySpecMap::containsKey)
        .collect(Collectors.toMap(Function.identity(), entitySpecMap::get));
  }

  @Override
  public Map<String, EntitySpec> getAllEntitySpecs() {
    return Collections.unmodifiableMap(entitySpecMap);
  }

  @Override
  public Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds) {
    return StreamSupport.stream(featureIds.spliterator(), false)
        .filter(featureSpecMap::containsKey)
        .collect(Collectors.toMap(Function.identity(), featureSpecMap::get));
  }

  @Override
  public Map<String, FeatureSpec> getAllFeatureSpecs() {
    return Collections.unmodifiableMap(featureSpecMap);
  }

  @Override
  public Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds) {
    return StreamSupport.stream(storageIds.spliterator(), false)
        .filter(storageSpecMap::containsKey)
        .collect(Collectors.toMap(Function.identity(), storageSpecMap::get));
  }

  @Override
  public Map<String, StorageSpec> getAllStorageSpecs() {
    return Collections.unmodifiableMap(storageSpecMap);
  }

  @Override
  public boolean isConnected() {
    return true;
  }
}
