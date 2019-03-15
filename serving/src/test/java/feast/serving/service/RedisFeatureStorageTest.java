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
import feast.serving.model.FeatureValue;
import feast.serving.testutil.RedisPopulator;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.Arrays;
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
            .setId("entity.feature_1")
            .setEntity(entityName)
            .setValueType(ValueType.Enum.STRING)
            .build();

    FeatureSpec featureSpec2 =
        FeatureSpec.newBuilder()
            .setId("entity.feature_2")
            .setEntity(entityName)
            .setValueType(ValueType.Enum.STRING)
            .build();

    List<FeatureSpec> featureSpecs = Arrays.asList(featureSpec1, featureSpec2);
    redisPopulator.populate(entityName, entityIds, featureSpecs, now);

    // add entity without feature
    List<String> requestEntityIds = new ArrayList<>(entityIds);
    requestEntityIds.add("100");
    List<FeatureValue> result = redisFs.getFeature(entityName, requestEntityIds, featureSpecs);
    redisPopulator.validate(result, entityIds, featureSpecs);
  }

  @Test
  public void getFeatures_shouldReturnLastValue() {
    FeatureSpec spec1 = createFeatureSpec("feature_1");
    FeatureSpec spec2 = createFeatureSpec("feature_2");
    List<FeatureSpec> featureSpecs = Arrays.asList(spec1, spec2);

    redisPopulator.populate(entityName, entityIds, featureSpecs, now);

    List<FeatureValue> result = redisFs.getFeature(entityName, entityIds, featureSpecs);

    redisPopulator.validate(result, entityIds, featureSpecs);
  }

  private FeatureSpec createFeatureSpec(String featureName) {
    return createFeatureSpec(featureName, ValueType.Enum.STRING, servingDatastoreSpec);
  }

  private FeatureSpec createFeatureSpec(
      String featureName, ValueType.Enum valType) {
    String entityName = "entity";
    String featureId = String.format("%s.%s", entityName, featureName);
    FeatureSpec spec =
        FeatureSpec.newBuilder()
            .setEntity(entityName)
            .setId(featureId)
            .setName(featureName)
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
