package feast.serving.service;

import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataSet;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataSetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.testutil.FakeRedisCoreService;
import feast.serving.testutil.RedisPopulator;
import feast.types.FeatureProto.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisServer;

public class RedisFeastServingTest {

  @Mock
  private SpecStorage coreService;

  // Embedded redis
  RedisServer redisServer;
  RedisPopulator redisPopulator;

  // Class under test
  RedisFeastServing redisFeastServing;

  private static final String STORE_ID = "SERVING";

  private static final String FEATURE_SET_NAME = "feature_set_1";
  private static final String FEATURE_SET_VER = "1";
  private static final String FN_TIMESTAMP = "timestamp";
  private static final String FN_REGION = "region";
  private static final String FN_DRIVER_ID = "driver_id";
  private static final String FN_FEATURE_1 = "feature_1";

  private static final long FN_TIMESTAMP_VAL = System.currentTimeMillis();
  private static final String FN_REGION_VAL = "id";
  private static final String FN_DRIVER_ID_VAL = "100";
  private static final int FN_FEATURE_1_VAL = 10;

  private JedisPool jedispool;
  private List<Field> fields;
  private FeatureSetSpec featureSetSpec;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    coreService = new FakeRedisCoreService();
    String redisHost = coreService.getStoreDetails(STORE_ID).getRedisConfig().getHost();
    int redisPort = coreService.getStoreDetails(STORE_ID).getRedisConfig().getPort();
    redisServer = new RedisServer(redisPort);

    try {
      redisServer.start();
    } catch (Exception e) {
      System.out.println(e);
      System.out.println("Unable to start redis redisServer");
      TestCase.fail();
    }

    // Setup JedisPool
    jedispool = new JedisPool(redisHost, redisPort);
    redisFeastServing = new RedisFeastServing(jedispool, GlobalTracer.get());
    redisPopulator = new RedisPopulator(redisHost, redisPort);

    // Initialise a FeatureSetSpec (Source is not set)
    featureSetSpec = FeatureSetSpec.newBuilder().setMaxAge(Long.MAX_VALUE).setName(FEATURE_SET_NAME)
        .setVersion(1)
        .addEntities(EntitySpec.newBuilder().setValueTypeValue(Value.STRING_VAL_FIELD_NUMBER)
            .setName(FN_REGION))
        .addEntities(EntitySpec.newBuilder().setValueTypeValue(Value.STRING_VAL_FIELD_NUMBER)
            .setName(FN_DRIVER_ID))
        .addFeatures(FeatureSpec.newBuilder().setValueTypeValue(Value.INT32_VAL_FIELD_NUMBER)
            .setName(FN_FEATURE_1)).build();

    // Populate redis with testing data
    Field field1 = Field.newBuilder().setName(FN_TIMESTAMP)
        .setValue(Value.newBuilder().setInt64Val(FN_TIMESTAMP_VAL).build()).build();
    Field field2 = Field.newBuilder().setName(FN_REGION)
        .setValue(Value.newBuilder().setStringVal(FN_REGION_VAL).build()).build();
    Field field3 = Field.newBuilder().setName(FN_DRIVER_ID)
        .setValue(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL)).build();
    Field field4 = Field.newBuilder().setName(FN_FEATURE_1)
        .setValue(Value.newBuilder().setInt32Val(FN_FEATURE_1_VAL)).build();

    fields = new ArrayList<>();
    fields.add(field1);
    fields.add(field2);
    fields.add(field3);
    fields.add(field4);

    redisPopulator.populate(fields, featureSetSpec,
        String.format("%s:%s", FEATURE_SET_NAME, FEATURE_SET_VER));
  }

  @After
  public void tearDown() {
    redisServer.stop();
  }

  @Test
  public void getOnlineFeatures_shouldPassFeatureRowsEqual() {
    FeatureSet featureSet = getFeatureSet();
    EntityDataSet entityDataSet = getEntityDataSet();
    FeatureRow featureRow = getFeatureRow();

    // Build GetFeatureRequest
    GetFeaturesRequest request = GetFeaturesRequest.newBuilder().addFeatureSets(featureSet)
        .setEntityDataSet(entityDataSet).build();
    GetOnlineFeaturesResponse response = redisFeastServing.getOnlineFeatures(request);

    Assert.assertEquals(featureRow, response.getFeatureDataSets(0).getFeatureRows(0));
  }

  @Test
  public void getOnlineFeatures_shouldPassNoResult() {
    // Construct GetFeatureRequest object
    FeatureSet featureSet = getFeatureSet();

    // Adding an additional entity name and value
    EntityDataSetRow entityDataSetRow = EntityDataSetRow.newBuilder(getEntityDataSetRow())
        .addValue(Value.newBuilder().setInt64Val(999L)).build();

    EntityDataSet entityDataSet = EntityDataSet.newBuilder(getEntityDataSet(entityDataSetRow))
        .addFieldNames("some_random_entitiy_name").build();

    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
        .addFeatureSets(featureSet).setEntityDataSet(entityDataSet).build();

    GetOnlineFeaturesResponse response = redisFeastServing.getOnlineFeatures(request);

    // Key does not exist in Redis, FeatureRow size == 0
    Assert.assertEquals(0, response.getFeatureDataSetsList().get(0).getFeatureRowsCount());
  }

  private FeatureSet getFeatureSet() {
    return FeatureSet.newBuilder().setName(FEATURE_SET_NAME)
        .setVersion(FEATURE_SET_VER).addFeatureNames(FN_FEATURE_1).build();
  }

  private EntityDataSetRow getEntityDataSetRow() {
    return EntityDataSetRow.newBuilder()
        .addValue(Value.newBuilder().setInt64Val(System.currentTimeMillis()))
        .addValue(Value.newBuilder().setStringVal(FN_REGION_VAL))
        .addValue(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL)).build();
  }

  private EntityDataSet getEntityDataSet() {
    return EntityDataSet.newBuilder()
        .addFieldNames(FN_REGION)
        .addFieldNames(FN_DRIVER_ID)
        .addEntityDataSetRows(getEntityDataSetRow()).build();
  }

  private EntityDataSet getEntityDataSet(EntityDataSetRow entityDataSetRow) {
    return EntityDataSet.newBuilder()
        .addFieldNames(FN_REGION)
        .addFieldNames(FN_DRIVER_ID)
        .addEntityDataSetRows(entityDataSetRow).build();
  }

  private FeatureRow getFeatureRow() {
    return FeatureRow.newBuilder().addAllFields(fields)
        .setEventTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
        .setFeatureSet(String.format("%s:%s", FEATURE_SET_NAME, FEATURE_SET_VER)).build();
  }

}
