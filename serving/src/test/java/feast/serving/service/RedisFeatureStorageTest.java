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

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.model.FeatureValue;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisServer;

public class RedisFeatureStorageTest {

  public static final String REDIS_HOST = "localhost";
  public static final int REDIS_PORT = 6377;

  // embedded redis
  RedisServer redisServer;
  RedisPopulator redisPopulator;

  // class under test
  RedisFeatureStorage redisFs;

  private List<String> entityIds;
  private String entityName;
  private Timestamp now;

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

    now = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();
  }

  @Test
  public void getFeatures_shouldNotReturnMissingValue() {
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
    redisPopulator.populate(entityName, entityIds, featureSpecs, now);

    // add entity without feature
    List<String> requestEntityIds = new ArrayList<>(entityIds);
    requestEntityIds.add("100");
    List<FeatureValue> result =
        redisFs.getFeature(entityName, requestEntityIds, featureSpecs, null);
    redisPopulator.validate(result, entityIds, featureSpecs, null);
  }

  @Test
  public void getFeatures_shouldReturnLastValue() {
    for (Granularity.Enum granularity : Granularity.Enum.values()) {
      if (granularity.equals(Enum.NONE) || granularity.equals(Enum.UNRECOGNIZED)) {
        continue;
      }
      FeatureSpec spec1 = createFeatureSpec("feature_1", Enum.NONE);
      FeatureSpec spec2 = createFeatureSpec("feature_2", granularity);
      List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);

      redisPopulator.populate(entityName, entityIds, featureSpecs, now);

      List<FeatureValue> result = redisFs.getFeature(entityName, entityIds, featureSpecs, null);
      List<FeatureValue> result2 = redisFs.getFeature(entityName, entityIds, featureSpecs, TimestampRange.getDefaultInstance());

      redisPopulator.validate(result, entityIds, featureSpecs, null);
      redisPopulator.validate(result2, entityIds, featureSpecs, null);
    }
  }

  @Test
  public void getFeatures_shouldReturnLastValueFilteredByTimestampIfSpecified() {
    String entityIdWithOldData = "entity_old";
    Timestamp fiveMinutesAgo = Timestamps.subtract(now, Durations.fromSeconds(300));

    Timestamp start = Timestamps.subtract(now, Durations.fromSeconds(60));
    Timestamp end = now;
    TimestampRange tsRange = TimestampRange.newBuilder().setStart(start).setEnd(end).build();

    Granularity.Enum granularity = Enum.SECOND;
    FeatureSpec spec1 = createFeatureSpec("feature_1", granularity);
    FeatureSpec spec2 = createFeatureSpec("feature_2", granularity);
    List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);

    redisPopulator.populate(entityName, entityIds, featureSpecs, now);
    redisPopulator.populate(
        entityName, Collections.singletonList(entityIdWithOldData), featureSpecs, fiveMinutesAgo);

    List<String> allEntityIds = new ArrayList<>(entityIds);
    allEntityIds.add(entityIdWithOldData);

    List<FeatureValue> result = redisFs.getFeature(entityName, allEntityIds, featureSpecs, tsRange);
    redisPopulator.validate(result, entityIds, featureSpecs, tsRange);
  }

  private FeatureSpec createFeatureSpec(String featureName, Enum granularity) {
    DataStore servingDatastoreSpec = DataStore.newBuilder().setId("REDIS").build();
    return createFeatureSpec(featureName, granularity, ValueType.Enum.STRING, servingDatastoreSpec);
  }

  private FeatureSpec createFeatureSpec(
      String featureName, Enum granularity, ValueType.Enum valType, DataStore dataStoreSpec) {
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
