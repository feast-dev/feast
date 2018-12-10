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

import static junit.framework.TestCase.fail;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.model.FeatureValue;
import feast.serving.model.Pair;
import feast.serving.testutil.RedisPopulator;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.GranularityProto.Granularity;
import feast.types.GranularityProto.Granularity.Enum;
import feast.types.ValueProto.ValueType;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisServer;

public class RedisFeatureStorageTest {

  public static final String REDIS_HOST = "localhost";
  public static final int REDIS_PORT = 6377;
  private static final int NUM_OF_DAYS_DATA = 2;

  // embedded redis
  RedisServer redisServer;
  RedisPopulator redisPopulator;

  // class under test
  RedisFeatureStorage redisFs;

  private List<String> entityIds;
  private String entityName;
  private Timestamp end;
  private Timestamp start;

  @Before
  public void setUp() throws Exception {
    redisServer = new RedisServer(REDIS_PORT);
    try {
      redisServer.start();
    } catch (Exception e) {
      System.out.println("Unable to start redis redisServer");
      fail();
    }

    JedisPool jedisPool = new JedisPool(REDIS_HOST, REDIS_PORT);
    redisFs = new RedisFeatureStorage(jedisPool, GlobalTracer.get());

    redisPopulator = new RedisPopulator(REDIS_HOST, REDIS_PORT);
    entityIds = createEntityIds(10);
    entityName = "entity";

    end = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();
    start = Timestamps.subtract(end, Durations.fromSeconds(NUM_OF_DAYS_DATA * 24 * 60 * 60));
  }

  @Test
  public void getCurrentFeature_shouldWorkForGranularityNone() {
    FeatureSpec featureSpec =
        FeatureSpec.newBuilder()
            .setId("entity.none.feature_1")
            .setEntity(entityName)
            .setGranularity(Enum.NONE)
            .setValueType(ValueType.Enum.STRING)
            .build();

    List<FeatureSpec> featureSpecs = Collections.singletonList(featureSpec);
    redisPopulator.populate(entityName, entityIds, featureSpecs, null, null);

    List<FeatureValue> result = redisFs.getCurrentFeature(entityName, entityIds, featureSpec);
    redisPopulator.validateCurrentValueGranularityNone(result, entityIds, featureSpecs);
  }

  @Test
  public void getCurrentFeature_shouldNotReturnMissingValue() {
    FeatureSpec featureSpec =
        FeatureSpec.newBuilder()
            .setId("entity.none.feature_1")
            .setEntity(entityName)
            .setGranularity(Enum.NONE)
            .setValueType(ValueType.Enum.STRING)
            .build();

    List<FeatureSpec> featureSpecs = Collections.singletonList(featureSpec);
    redisPopulator.populate(entityName, entityIds, featureSpecs, null, null);

    // add entity without feature
    List<String> requestEntityIds = new ArrayList<>(entityIds);
    requestEntityIds.add("100");
    List<FeatureValue> result =
        redisFs.getCurrentFeature(entityName, requestEntityIds, featureSpec);
    redisPopulator.validateCurrentValueGranularityNone(result, entityIds, featureSpecs);
  }

  @Test
  public void getCurrentFeature_shouldReturnLastValueForOtherGranularity() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec =
          createFeatureSpec("feature_1",granularity);

      List<FeatureSpec> featureSpecs = Collections.singletonList(spec);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      List<FeatureValue> result = redisFs.getCurrentFeature(entityName, entityIds, spec);
      redisPopulator.validateCurrentValueOtherGranularity(result, entityIds, featureSpecs, end);
    }
  }


  @Test
  public void getCurrentFeatures_shouldWorkForGranularityNone() {
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setId("entity.none.feature_1")
            .setEntity(entityName)
            .setGranularity(Enum.NONE)
            .setValueType(ValueType.Enum.STRING)
            .build();

    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId("entity.none.feature_2")
            .setEntity(entityName)
            .setGranularity(Enum.NONE)
            .setValueType(ValueType.Enum.STRING)
            .build();

    List<FeatureSpec> featureSpecs = Arrays.asList(featureSpec1, featureSpec2);
    redisPopulator.populate(entityName, entityIds, featureSpecs, null, null);

    List<FeatureValue> result = redisFs.getCurrentFeatures(entityName, entityIds, featureSpecs);
    redisPopulator.validateCurrentValueGranularityNone(result, entityIds, featureSpecs);
  }

  @Test
  public void getCurrentFeatures_shouldNotReturnMissingValue() {
    FeatureSpec featureSpec1 =
        FeatureSpec.newBuilder()
            .setId("entity.none.feature_1")
            .setEntity(entityName)
            .setGranularity(Enum.NONE)
            .setValueType(ValueType.Enum.STRING)
            .build();

    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId("entity.none.feature_2")
            .setEntity(entityName)
            .setGranularity(Enum.NONE)
            .setValueType(ValueType.Enum.STRING)
            .build();

    List<FeatureSpec> featureSpecs = Arrays.asList(featureSpec1, featureSpec2);
    redisPopulator.populate(entityName, entityIds, featureSpecs, null, null);

    // add entity without feature
    List<String> requestEntityIds = new ArrayList<>(entityIds);
    requestEntityIds.add("100");
    List<FeatureValue> result =
        redisFs.getCurrentFeatures(entityName, requestEntityIds, featureSpecs);
    redisPopulator.validateCurrentValueGranularityNone(result, entityIds, featureSpecs);
  }

  @Test
  public void getCurrentFeatures_shouldReturnLastValueForOtherGranularity() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec1 =
          createFeatureSpec("feature_1", granularity);
      FeatureSpec spec2 =
          createFeatureSpec("feature_2", granularity);
      List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);

      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      List<FeatureValue> result = redisFs.getCurrentFeatures(entityName, entityIds, featureSpecs);
      redisPopulator.validateCurrentValueOtherGranularity(result, entityIds, featureSpecs, end);
    }
  }

  @Test
  public void getNLatestFeatureWithinTimerange_shouldWorkForTimeseriesData() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec =
          createFeatureSpec("feature_1", granularity);

      List<FeatureSpec> featureSpecs = Collections.singletonList(spec);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      TimestampRange tsRange = TimestampRange.newBuilder().setStart(start).setEnd(end).build();
      int limit = 5;
      Pair<FeatureSpec, Integer> featureSpecLimitPair = new Pair<>(spec, limit);
      List<FeatureValue> result =
          redisFs.getNLatestFeatureWithinTimestampRange(
              entityName, entityIds, featureSpecLimitPair, tsRange);
      redisPopulator.validateValueWithinTimerange(
          result, entityIds, Collections.singletonList(featureSpecLimitPair), tsRange);
    }
  }

  @Test
  public void getNLatestFeatureWithinTimerange_shouldNotReturnMissingEntity() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec =
          createFeatureSpec("feature_1", granularity);

      List<FeatureSpec> featureSpecs = Collections.singletonList(spec);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      TimestampRange tsRange = TimestampRange.newBuilder().setStart(start).setEnd(end).build();
      int limit = 5;
      Pair<FeatureSpec, Integer> featureSpecLimitPair = new Pair<>(spec, limit);

      List<String> entityIdsWithMissing = new ArrayList<>(entityIds);
      entityIdsWithMissing.add("100"); // missing entity
      List<FeatureValue> result =
          redisFs.getNLatestFeatureWithinTimestampRange(
              entityName, entityIdsWithMissing, featureSpecLimitPair, tsRange);
      redisPopulator.validateValueWithinTimerange(
          result, entityIds, Collections.singletonList(featureSpecLimitPair), tsRange);
    }
  }

  @Test
  public void getNLatestFeatureWithinTimerange_shouldNotReturnMissingValueInCertainTimestamp() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec =
          createFeatureSpec("feature_1", granularity);

      List<FeatureSpec> featureSpecs = Collections.singletonList(spec);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      Timestamp earlierStart = Timestamps.subtract(start, Durations.fromSeconds(86400));
      TimestampRange tsRange =
          TimestampRange.newBuilder().setStart(earlierStart).setEnd(end).build();
      int limit = 5;
      Pair<FeatureSpec, Integer> featureSpecLimitPair = new Pair<>(spec, limit);

      List<FeatureValue> result =
          redisFs.getNLatestFeatureWithinTimestampRange(
              entityName, entityIds, featureSpecLimitPair, tsRange);
      redisPopulator.validateValueWithinTimerange(
          result, entityIds, Collections.singletonList(featureSpecLimitPair), tsRange);
    }
  }

  @Test
  public void getNLatestFeatureWithinTimerange_shouldReturnEmptyListIfNoFeatureIsRetrieved() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec =
          createFeatureSpec("feature_1", granularity);

      List<FeatureSpec> featureSpecs = Collections.singletonList(spec);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      Timestamp earlierStart = Timestamps.subtract(start, Durations.fromSeconds(86400));
      TimestampRange tsRange =
          TimestampRange.newBuilder().setStart(earlierStart).setEnd(end).build();
      int limit = 5;
      Pair<FeatureSpec, Integer> featureSpecLimitPair = new Pair<>(spec, limit);

      String missingEntity = "missingEntity";
      List<FeatureValue> result =
          redisFs.getNLatestFeatureWithinTimestampRange(
              entityName, Arrays.asList(missingEntity), featureSpecLimitPair, tsRange);
      assertThat(result.size(), equalTo(0));
    }
  }


  @Test
  public void getNLatestFeaturesWithinTimerange_shouldWorkForTimeseriesData() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec1 =
          createFeatureSpec("feature_1", granularity);
      FeatureSpec spec2 =
          createFeatureSpec("feature_2", granularity);
      List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      TimestampRange tsRange = TimestampRange.newBuilder().setStart(start).setEnd(end).build();
      int limit1 = 2;
      int limit2 = 3;
      Pair<FeatureSpec, Integer> featureSpecLimitPair1 = new Pair<>(spec1, limit1);
      Pair<FeatureSpec, Integer> featureSpecLimitPair2 = new Pair<>(spec2, limit2);
      List<Pair<FeatureSpec, Integer>> featureSpecLimitPairs = Arrays.asList(featureSpecLimitPair1, featureSpecLimitPair2);
      List<FeatureValue> result =
          redisFs.getNLatestFeaturesWithinTimestampRange(
              entityName, entityIds, featureSpecLimitPairs, tsRange);
      redisPopulator.validateValueWithinTimerange(
          result, entityIds, featureSpecLimitPairs, tsRange);
    }
  }

  @Test
  public void getNLatestFeaturesWithinTimerange_shouldNotReturnMissingEntity() {

    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec1 =
          createFeatureSpec("feature_1", granularity);
      FeatureSpec spec2 =
          createFeatureSpec("feature_2", granularity);
      List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      TimestampRange tsRange = TimestampRange.newBuilder().setStart(start).setEnd(end).build();
      int limit1 = 5;
      int limit2 = 1;
      Pair<FeatureSpec, Integer> featureSpecLimitPair1 = new Pair<>(spec1, limit1);
      Pair<FeatureSpec, Integer> featureSpecLimitPair2 = new Pair<>(spec2, limit2);
      List<Pair<FeatureSpec, Integer>> featureSpecLimitPairs = Arrays.asList(featureSpecLimitPair1, featureSpecLimitPair2);

      List<String> entityIdsWithMissing = new ArrayList<>(entityIds);
      entityIdsWithMissing.add("100"); // missing entity
      List<FeatureValue> result =
          redisFs.getNLatestFeaturesWithinTimestampRange(
              entityName, entityIdsWithMissing, featureSpecLimitPairs, tsRange);
      redisPopulator.validateValueWithinTimerange(
          result, entityIds, featureSpecLimitPairs, tsRange);
    }
  }

  @Test
  public void getNLatestFeaturesWithinTimerange_shouldNotReturnMissingValueInCertainTimestamp() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec1 =
          createFeatureSpec("feature_1", granularity);
      FeatureSpec spec2 =
          createFeatureSpec("feature_2", granularity);
      List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      Timestamp earlierStart = Timestamps.subtract(start, Durations.fromSeconds(86400));
      TimestampRange tsRange =
          TimestampRange.newBuilder().setStart(earlierStart).setEnd(end).build();
      int limit1 = 5;
      int limit2 = 10;
      Pair<FeatureSpec, Integer> featureSpecLimitPair1 = new Pair<>(spec1, limit1);
      Pair<FeatureSpec, Integer> featureSpecLimitPair2 = new Pair<>(spec2, limit2);
      List<Pair<FeatureSpec, Integer>> featureSpecLimitPairs = Arrays.asList(featureSpecLimitPair1, featureSpecLimitPair2);

      List<FeatureValue> result =
          redisFs.getNLatestFeaturesWithinTimestampRange(
              entityName, entityIds, featureSpecLimitPairs, tsRange);
      redisPopulator.validateValueWithinTimerange(
          result, entityIds, featureSpecLimitPairs, tsRange);
    }
  }

  @Test
  public void getNLatestFeaturesWithinTimerange_shouldReturnEmptyListIfNoFeatureIsRetrieved() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec1 =
          createFeatureSpec("feature_1", granularity);
      FeatureSpec spec2 =
          createFeatureSpec("feature_2", granularity);
      List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);
      redisPopulator.populate(entityName, entityIds, featureSpecs, start, end);

      Timestamp earlierStart = Timestamps.subtract(start, Durations.fromSeconds(86400));
      TimestampRange tsRange =
          TimestampRange.newBuilder().setStart(earlierStart).setEnd(end).build();
      int limit1 = 5;
      int limit2 = 10;
      Pair<FeatureSpec, Integer> featureSpecLimitPair1 = new Pair<>(spec1, limit1);
      Pair<FeatureSpec, Integer> featureSpecLimitPair2 = new Pair<>(spec2, limit2);
      List<Pair<FeatureSpec, Integer>> featureSpecLimitPairs = Arrays.asList(featureSpecLimitPair1, featureSpecLimitPair2);

      String missingEntity = "missingEntity";
      List<FeatureValue> result =
          redisFs.getNLatestFeaturesWithinTimestampRange(
              entityName, Arrays.asList(missingEntity), featureSpecLimitPairs, tsRange);
      assertThat(result.size(), equalTo(0));
    }
  }

  private FeatureSpec createFeatureSpec(String featureName, Enum granularity) {
    DataStore servingDatastoreSpec =
        DataStore.newBuilder()
            .putOptions(
                RedisFeatureStorage.OPT_REDIS_BUCKET_SIZE,
                Duration.standardSeconds(86400).toString())
            .build();
    return createFeatureSpec(featureName, granularity, ValueType.Enum.STRING, servingDatastoreSpec);
  }

  private FeatureSpec createFeatureSpec(String featureName,
      Enum granularity, ValueType.Enum valType, DataStore dataStoreSpec) {
    String entityName = "entity";
    String featureId =
        String.format("%s.%s.%s", entityName, granularity.toString().toLowerCase(), featureName);
    FeatureSpec spec =
        FeatureSpec.newBuilder()
            .setDataStores(DataStores.newBuilder().setServing(dataStoreSpec))
            .setEntity(entityName)
            .setId(featureId)
            .setName(featureName)
            .setGranularity(granularity)
            .setValueType(valType)
            .build();

    return spec;
  }

  private List<String> createEntityIds(int count) {
    List<String> entityIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      entityIds.add(String.valueOf(i));
    }
    return entityIds;
  }

  @After
  public void tearDown() throws Exception {
    redisServer.stop();
  }
}
