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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataset;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.service.serving.RedisServingService;
import feast.serving.service.spec.CachedSpecService;
import feast.serving.service.spec.SpecService;
import feast.serving.testutil.RedisPopulator;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisServer;

public class RedisGrpcServingServiceTest {

  @Mock
  SpecService coreService;

  // Embedded redis
  RedisServer redisServer;
  RedisPopulator redisPopulator;

  // Class under test
  RedisServingService redisServingService;

  private static final String STORE_ID = "SERVING";
  private static final int REDIS_PORT = 63799;

  private static final String FEATURE_SET_NAME = "feature_set_1";
  private static final int FEATURE_SET_VER = 1;
  private static final String FN_REGION = "region";
  private static final String FN_DRIVER_ID = "driver_id";
  private static final String FN_FEATURE_1 = "feature_1";

  private static final String FN_REGION_VAL = "id";
  private static final String FN_DRIVER_ID_VAL = "100";
  private static final int FN_FEATURE_1_VAL = 10;


  private JedisPool jedispool;
  private List<Field> fields;
  private FeatureSetSpec featureSetSpec;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    redisServer = new RedisServer(REDIS_PORT);

    try {
      redisServer.start();
    } catch (Exception e) {
      System.out.println(e);
      System.out.println("Unable to start redis redisServer");
      TestCase.fail();
    }

    // Setup JedisPool
    jedispool = new JedisPool("localhost", REDIS_PORT);

    // Initialise a FeatureSetSpec (Source is not set)
    featureSetSpec = FeatureSetSpec.newBuilder()
        .setMaxAge(Duration.newBuilder().setSeconds(Long.MAX_VALUE))
        .setName(FEATURE_SET_NAME)
        .setVersion(1)
        .addEntities(EntitySpec.newBuilder().setValueTypeValue(Value.STRING_VAL_FIELD_NUMBER)
            .setName(FN_REGION))
        .addEntities(EntitySpec.newBuilder().setValueTypeValue(Value.STRING_VAL_FIELD_NUMBER)
            .setName(FN_DRIVER_ID))
        .addFeatures(FeatureSpec.newBuilder().setValueTypeValue(Value.INT32_VAL_FIELD_NUMBER)
            .setName(FN_FEATURE_1)).build();
    Store store = Store.newBuilder()
        .setName(STORE_ID)
        .setType(StoreType.REDIS)
        .setRedisConfig(RedisConfig.newBuilder()
            .setHost("localhost")
            .setPort(REDIS_PORT))
        .addSubscriptions(Subscription.newBuilder().setName(FEATURE_SET_NAME).setVersion("1"))
        .build();

    Map<String, FeatureSetSpec> featureSetSpecs = new HashMap<>();
    featureSetSpecs.put(String.format("%s:%s", FEATURE_SET_NAME, FEATURE_SET_VER), featureSetSpec);

    when(coreService.getStoreDetails(STORE_ID)).thenReturn(store);
    when(coreService.getFeatureSetSpecs(store.getSubscriptionsList())).thenReturn(featureSetSpecs);

    CachedSpecService cachedSpecService = new CachedSpecService(coreService, STORE_ID);
    redisServingService = new RedisServingService(jedispool, cachedSpecService, GlobalTracer.get());
    redisPopulator = new RedisPopulator("localhost", REDIS_PORT);

    // Populate redis with testing data
    Field field1 = Field.newBuilder().setName(FN_REGION)
        .setValue(Value.newBuilder().setStringVal(FN_REGION_VAL)).build();
    Field field2 = Field.newBuilder().setName(FN_DRIVER_ID)
        .setValue(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL)).build();
    Field field3 = Field.newBuilder().setName(FN_FEATURE_1)
        .setValue(Value.newBuilder().setInt32Val(FN_FEATURE_1_VAL)).build();

    fields = new ArrayList<>();
    fields.add(field1);
    fields.add(field2);
    fields.add(field3);

    redisPopulator.populate(fields, featureSetSpec, getFeatureRow());

  }

  @After
  public void tearDown() {
    redisServer.stop();
  }

  @Test
  public void getOnlineFeatures_shouldPassIfKeyFound() {
    FeatureSet featureSet = getFeatureSet();
    EntityDataset entityDataset = getEntityDataset();
    FeatureRow featureRow = getFeatureRow();

    // Build GetFeatureRequest
    GetFeaturesRequest request = GetFeaturesRequest.newBuilder().addFeatureSets(featureSet)
        .setEntityDataset(entityDataset).build();
    GetOnlineFeaturesResponse response = redisServingService.getOnlineFeatures(request);

    assertThat(response.getFeatureDatasets(0).getFeatureRows(0), equalTo(featureRow));
  }

  @Test
  public void getOnlineFeatures_shouldPassIfKeyNotFound() {
    // Construct GetFeatureRequest object
    FeatureSet featureSet = getFeatureSet();
    String invalidEntityKey = "some_random_entitiy_name";

    // Adding an additional entity name and value
    EntityDatasetRow entityDataSetRow = EntityDatasetRow.newBuilder(getEntityDatasetRow())
        .addEntityIds(Value.newBuilder().setInt64Val(999L)).build();

    EntityDataset entityDataSet = EntityDataset.newBuilder(getEntityDataset(entityDataSetRow))
        .addEntityNames(invalidEntityKey).build();

    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
        .addFeatureSets(featureSet).setEntityDataset(entityDataSet).build();

    GetOnlineFeaturesResponse response = redisServingService.getOnlineFeatures(request);

    FeatureRow emptyFeatureRow = FeatureRow.newBuilder()
        .addFields(Field.newBuilder().setName(FN_REGION)
            .setValue(Value.newBuilder().setStringVal(FN_REGION_VAL)))
        .addFields(Field.newBuilder().setName(FN_DRIVER_ID)
            .setValue(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL)))
        .addFields(Field.newBuilder().setName(invalidEntityKey)
            .setValue(Value.newBuilder().setInt64Val(999L)))
        .addFields(Field.newBuilder().setName(FN_FEATURE_1))
        .setFeatureSet(String.format("%s:%s", FEATURE_SET_NAME, FEATURE_SET_VER))
        .setEventTimestamp(Timestamp.newBuilder())
        .build();
    assertThat(response.getFeatureDatasetsList().get(0).getFeatureRows(0),
        equalTo(emptyFeatureRow));
  }

  private FeatureSet getFeatureSet() {
    return FeatureSet.newBuilder().setName(FEATURE_SET_NAME)
        .setVersion(FEATURE_SET_VER).addFeatureNames(FN_FEATURE_1).build();
  }

  private EntityDatasetRow getEntityDatasetRow() {
    return EntityDatasetRow.newBuilder()
        .addEntityIds(Value.newBuilder().setStringVal(FN_REGION_VAL))
        .addEntityIds(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL)).build();
  }

  private EntityDataset getEntityDataset() {
    return EntityDataset.newBuilder()
        .addEntityNames(FN_REGION)
        .addEntityNames(FN_DRIVER_ID)
        .addEntityDatasetRows(getEntityDatasetRow()).build();
  }

  private EntityDataset getEntityDataset(EntityDatasetRow entityDataSetRow) {
    return EntityDataset.newBuilder()
        .addEntityNames(FN_REGION)
        .addEntityNames(FN_DRIVER_ID)
        .addEntityDatasetRows(entityDataSetRow).build();
  }

  private FeatureRow getFeatureRow() {
    return FeatureRow.newBuilder().addAllFields(fields)
        .setEventTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
        .setFeatureSet(String.format("%s:%s", FEATURE_SET_NAME, FEATURE_SET_VER)).build();
  }

}
