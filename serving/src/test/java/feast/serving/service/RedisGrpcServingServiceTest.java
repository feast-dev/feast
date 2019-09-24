// /*
//  * Copyright 2018 The Feast Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     https://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  *
//  */
//
// package feast.serving.service;
//
// import com.google.protobuf.Timestamp;
// import feast.core.FeatureSetProto.EntitySpec;
// import feast.core.FeatureSetProto.FeatureSetSpec;
// import feast.core.FeatureSetProto.FeatureSpec;
// import feast.serving.ServingAPIProto.GetFeaturesRequest;
// import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataSet;
// import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataSetRow;
// import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
// import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
// import feast.serving.service.serving.RedisServingService;
// import feast.serving.service.spec.SpecService;
// import feast.serving.testutil.FakeRedisCoreService;
// import feast.serving.testutil.RedisPopulator;
// import feast.types.FeatureRowProto.FeatureRow;
// import feast.types.FieldProto.Field;
// import feast.types.ValueProto.Value;
// import io.opentracing.util.GlobalTracer;
// import java.util.ArrayList;
// import java.util.List;
// import junit.framework.TestCase;
// import org.junit.After;
// import org.junit.Assert;
// import org.junit.Before;
// import org.junit.Test;
// import org.mockito.Mock;
// import org.mockito.MockitoAnnotations;
// import redis.clients.jedis.JedisPool;
// import redis.embedded.RedisServer;
//
// public class RedisGrpcServingServiceTest {
//
//   @Mock
//   SpecService coreService;
//
//   // Embedded redis
//   RedisServer redisServer;
//   RedisPopulator redisPopulator;
//
//   // Class under test
//   RedisServingService redisServingService;
//
//   private static final String STORE_ID = "SERVING";
//
//   private static final String FEATURE_SET_NAME = "feature_set_1";
//   private static final String FEATURE_SET_VER = "1";
//   private static final String FN_REGION = "region";
//   private static final String FN_DRIVER_ID = "driver_id";
//   private static final String FN_FEATURE_1 = "feature_1";
//
//   private static final String FN_REGION_VAL = "id";
//   private static final String FN_DRIVER_ID_VAL = "100";
//   private static final int FN_FEATURE_1_VAL = 10;
//
//
//   private JedisPool jedispool;
//   private List<Field> fields;
//   private FeatureSetSpec featureSetSpec;
//
//   @Before
//   public void setUp() throws Exception {
//     MockitoAnnotations.initMocks(this);
//     coreService = new FakeRedisCoreService();
//     String redisHost = coreService.getStoreDetails(STORE_ID).getRedisConfig().getHost();
//     int redisPort = coreService.getStoreDetails(STORE_ID).getRedisConfig().getPort();
//     redisServer = new RedisServer(redisPort);
//
//     try {
//       redisServer.start();
//     } catch (Exception e) {
//       System.out.println(e);
//       System.out.println("Unable to start redis redisServer");
//       TestCase.fail();
//     }
//
//     // Setup JedisPool
//     jedispool = new JedisPool(redisHost, redisPort);
//     redisServingService = new RedisServingService(jedispool, GlobalTracer.get());
//     redisPopulator = new RedisPopulator(redisHost, redisPort);
//
//     // Initialise a FeatureSetSpec (Source is not set)
//     featureSetSpec = FeatureSetSpec.newBuilder().setMaxAge(Long.MAX_VALUE).setName(FEATURE_SET_NAME)
//         .setVersion(1)
//         .addEntities(EntitySpec.newBuilder().setValueTypeValue(Value.STRING_VAL_FIELD_NUMBER)
//             .setName(FN_REGION))
//         .addEntities(EntitySpec.newBuilder().setValueTypeValue(Value.STRING_VAL_FIELD_NUMBER)
//             .setName(FN_DRIVER_ID))
//         .addFeatures(FeatureSpec.newBuilder().setValueTypeValue(Value.INT32_VAL_FIELD_NUMBER)
//             .setName(FN_FEATURE_1)).build();
//
//     // Populate redis with testing data
//     Field field1 = Field.newBuilder().setName(FN_REGION)
//         .setValue(Value.newBuilder().setStringVal(FN_REGION_VAL)).build();
//     Field field2 = Field.newBuilder().setName(FN_DRIVER_ID)
//         .setValue(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL)).build();
//     Field field3 = Field.newBuilder().setName(FN_FEATURE_1)
//         .setValue(Value.newBuilder().setInt32Val(FN_FEATURE_1_VAL)).build();
//
//     fields = new ArrayList<>();
//     fields.add(field1);
//     fields.add(field2);
//     fields.add(field3);
//
//     redisPopulator.populate(fields, featureSetSpec, getFeatureRow());
//   }
//
//   @After
//   public void tearDown() {
//     redisServer.stop();
//   }
//
//   @Test
//   public void getOnlineFeatures_shouldPassIfKeyFound() {
//     FeatureSet featureSet = getFeatureSet();
//     EntityDataSet entityDataSet = getEntityDataSet();
//     FeatureRow featureRow = getFeatureRow();
//
//     // Build GetFeatureRequest
//     GetFeaturesRequest request = GetFeaturesRequest.newBuilder().addFeatureSets(featureSet)
//         .setEntityDataSet(entityDataSet).build();
//     GetOnlineFeaturesResponse response = redisServingService.getOnlineFeatures(request);
//
//     Assert.assertEquals(featureRow, response.getFeatureDataSets(0).getFeatureRows(0));
//   }
//
// //  @Test
// //  public void getOnlineFeatures_shouldPassIfKeyNotFound() {
// //    // Construct GetFeatureRequest object
// //    FeatureSet featureSet = getFeatureSet();
// //
// //    // Adding an additional entity name and value
// //    EntityDataSetRow entityDataSetRow = EntityDataSetRow.newBuilder(getEntityDataSetRow())
// //        .addValue(Value.newBuilder().setInt64Val(999L)).build();
// //
// //    EntityDataSet entityDataSet = EntityDataSet.newBuilder(getEntityDataSet(entityDataSetRow))
// //        .addFieldNames("some_random_entitiy_name").build();
// //
// //    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
// //        .addFeatureSets(featureSet).setEntityDataSet(entityDataSet).build();
// //
// // GetOnlineFeaturesResponse response = redisServingService.getOnlineFeatures(request);
// //
// //    // Key does not exist in Redis, FeatureRow size == 0
// //    Assert.assertEquals(0, response.getFeatureDataSetsList().get(0).getFeatureRowsCount());
// //  }
//
//   private FeatureSet getFeatureSet() {
//     return FeatureSet.newBuilder().setName(FEATURE_SET_NAME)
//         .setVersion(FEATURE_SET_VER).addFeatureNames(FN_FEATURE_1).build();
//   }
//
//   private EntityDataSetRow getEntityDataSetRow() {
//     return EntityDataSetRow.newBuilder()
//         .addValue(Value.newBuilder().setStringVal(FN_REGION_VAL))
//         .addValue(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL)).build();
//   }
//
//   private EntityDataSet getEntityDataSet() {
//     return EntityDataSet.newBuilder()
//         .addFieldNames(FN_REGION)
//         .addFieldNames(FN_DRIVER_ID)
//         .addEntityDataSetRows(getEntityDataSetRow()).build();
//   }
//
//   private EntityDataSet getEntityDataSet(EntityDataSetRow entityDataSetRow) {
//     return EntityDataSet.newBuilder()
//         .addFieldNames(FN_REGION)
//         .addFieldNames(FN_DRIVER_ID)
//         .addEntityDataSetRows(entityDataSetRow).build();
//   }
//
//   private FeatureRow getFeatureRow() {
//     return FeatureRow.newBuilder().addAllFields(fields)
//         .setEventTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
//         .setFeatureSet(String.format("%s:%s", FEATURE_SET_NAME, FEATURE_SET_VER)).build();
//   }
//
// }
