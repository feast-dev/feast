package feast.serving.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureSetRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FieldValues;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisServingServiceTest {

  @Mock
  JedisPool jedisPool;

  @Mock
  Jedis jedis;

  @Mock
  CachedSpecService specService;

  @Mock
  Tracer tracer;

  private RedisServingService redisServingService;
  private byte[][] redisKeyList;

  @Before
  public void setUp() {
    initMocks(this);

    redisServingService = new RedisServingService(jedisPool, specService, tracer);
    redisKeyList = Lists.newArrayList(
        RedisKey.newBuilder().setFeatureSet("featureSet:1")
            .addAllEntities(Lists.newArrayList(
                Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                Field.newBuilder().setName("entity2").setValue(strValue("a")).build()
            )).build(),
        RedisKey.newBuilder().setFeatureSet("featureSet:1")
            .addAllEntities(Lists.newArrayList(
                Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                Field.newBuilder().setName("entity2").setValue(strValue("b")).build()
            )).build()
    ).stream()
        .map(AbstractMessageLite::toByteArray)
        .collect(Collectors.toList())
        .toArray(new byte[0][0]);
  }

  @Test
  public void shouldReturnResponseWithValuesIfKeysPresent() {
    GetOnlineFeaturesRequest request = GetOnlineFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSetRequest.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a")))
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b")))
        .build();

    List<FeatureRow> featureRows = Lists.newArrayList(
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
            .setFeatureSet("featureSet:1")
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
            .setFeatureSet("featureSet:1")
            .build()
    );

    List<byte[]> featureRowBytes = featureRows.stream()
        .map(AbstractMessageLite::toByteArray)
        .collect(Collectors.toList());
    when(specService.getFeatureSet("featureSet", 1)).thenReturn(getFeatureSetSpec());
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(FieldValues.newBuilder()
          .putFields("entity1", intValue(1))
          .putFields("entity2", strValue("a"))
          .putFields("featureSet:1:feature1", intValue(1))
          .putFields("featureSet:1:feature2", intValue(1)))
        .addFieldValues(FieldValues.newBuilder()
          .putFields("entity1", intValue(2))
          .putFields("entity2", strValue("b"))
          .putFields("featureSet:1:feature1", intValue(2))
          .putFields("featureSet:1:feature2", intValue(2)))
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldReturnResponseWithValuesWhenFeatureSetSpecHasUnspecifiedMaxAge() {
    GetOnlineFeaturesRequest request = GetOnlineFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSetRequest.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a")))
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b")))
        .build();

    List<FeatureRow> featureRows = Lists.newArrayList(
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(2)) // much older timestamp
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
            .setFeatureSet("featureSet:1")
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(15)) // much older timestamp
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
            .setFeatureSet("featureSet:1")
            .build()
    );

    List<byte[]> featureRowBytes = featureRows.stream()
        .map(AbstractMessageLite::toByteArray)
        .collect(Collectors.toList());
    when(specService.getFeatureSet("featureSet", 1)).thenReturn(getFeatureSetSpecWithNoMaxAge());
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(FieldValues.newBuilder()
          .putFields("entity1", intValue(1))
          .putFields("entity2", strValue("a"))
          .putFields("featureSet:1:feature1", intValue(1))
          .putFields("featureSet:1:feature2", intValue(1)))
        .addFieldValues(FieldValues.newBuilder()
          .putFields("entity1", intValue(2))
          .putFields("entity2", strValue("b"))
          .putFields("featureSet:1:feature1", intValue(2))
          .putFields("featureSet:1:feature2", intValue(2)))
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfKeysNotPresent() {
    // some keys not present, should have empty values
    GetOnlineFeaturesRequest request = GetOnlineFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSetRequest.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a")))
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b")))
        .build();

    List<FeatureRow> featureRows = Lists.newArrayList(
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
            .setFeatureSet("featureSet:1")
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder())
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                    Field.newBuilder().setName("feature1").build(),
                    Field.newBuilder().setName("feature2").build()))
            .setFeatureSet("featureSet:1")
            .build()
    );

    List<byte[]> featureRowBytes = Lists.newArrayList(featureRows.get(0).toByteArray(), null);
    when(specService.getFeatureSet("featureSet", 1)).thenReturn(getFeatureSetSpec());
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a"))
            .putFields("featureSet:1:feature1", intValue(1))
            .putFields("featureSet:1:feature2", intValue(1)))
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b"))
            .putFields("featureSet:1:feature1", Value.newBuilder().build())
            .putFields("featureSet:1:feature2", Value.newBuilder().build()))
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfMaxAgeIsExceeded() {
    // keys present, but too stale comp. to maxAge set in request
    GetOnlineFeaturesRequest request = GetOnlineFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSetRequest.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .setMaxAge(Duration.newBuilder().setSeconds(10))
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a")))
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b")))
        .build();

    List<FeatureRow> featureRows = Lists.newArrayList(
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
            .setFeatureSet("featureSet:1")
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(50)) // this value should be nulled
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
            .setFeatureSet("featureSet:1")
            .build()
    );

    List<byte[]> featureRowBytes = featureRows.stream()
        .map(AbstractMessageLite::toByteArray)
        .collect(Collectors.toList());
    when(specService.getFeatureSet("featureSet", 1)).thenReturn(getFeatureSetSpec());
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a"))
            .putFields("featureSet:1:feature1", intValue(1))
            .putFields("featureSet:1:feature2", intValue(1)))
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b"))
            .putFields("featureSet:1:feature1", Value.newBuilder().build())
            .putFields("featureSet:1:feature2", Value.newBuilder().build()))
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }


  @Test
  public void shouldReturnResponseWithUnsetValuesIfDefaultMaxAgeIsExceeded() {
    // keys present, but too stale comp. to maxAge set in featureSetSpec
    GetOnlineFeaturesRequest request = GetOnlineFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSetRequest.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a")))
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b")))
        .build();

    List<FeatureRow> featureRows = Lists.newArrayList(
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
            .setFeatureSet("featureSet:1")
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(0)) // this value should be nulled
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
            .setFeatureSet("featureSet:1")
            .build()
    );

    List<byte[]> featureRowBytes = featureRows.stream()
        .map(AbstractMessageLite::toByteArray)
        .collect(Collectors.toList());
    when(specService.getFeatureSet("featureSet", 1)).thenReturn(getFeatureSetSpec());
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a"))
            .putFields("featureSet:1:feature1", intValue(1))
            .putFields("featureSet:1:feature2", intValue(1)))
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b"))
            .putFields("featureSet:1:feature1", Value.newBuilder().build())
            .putFields("featureSet:1:feature2", Value.newBuilder().build()))
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }


  @Test
  public void shouldFilterOutUndesiredRows() {
    // requested rows less than the rows available in the featureset
    GetOnlineFeaturesRequest request = GetOnlineFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSetRequest.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1"))
            .build())
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a")))
        .addEntityRows(EntityRow.newBuilder()
            .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b")))
        .build();

    List<FeatureRow> featureRows = Lists.newArrayList(
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("a")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
            .setFeatureSet("featureSet:1")
            .build(),
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
            .addAllFields(Lists
                .newArrayList(
                    Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                    Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                    Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
            .setFeatureSet("featureSet:1")
            .build()
    );

    List<byte[]> featureRowBytes = featureRows.stream()
        .map(AbstractMessageLite::toByteArray)
        .collect(Collectors.toList());
    when(specService.getFeatureSet("featureSet", 1)).thenReturn(getFeatureSetSpec());
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(1))
            .putFields("entity2", strValue("a"))
            .putFields("featureSet:1:feature1", intValue(1)))
        .addFieldValues(FieldValues.newBuilder()
            .putFields("entity1", intValue(2))
            .putFields("entity2", strValue("b"))
            .putFields("featureSet:1:feature1", intValue(2)))
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(responseToMapList(actual), containsInAnyOrder(responseToMapList(expected).toArray()));
  }

  private List<Map<String, Value>> responseToMapList(GetOnlineFeaturesResponse response) {
    return response.getFieldValuesList().stream().map(FieldValues::getFieldsMap).collect(Collectors.toList());
  }

  private Value intValue(int val) {
    return Value.newBuilder().setInt64Val(val).build();
  }

  private Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }
  private FeatureSetSpec getFeatureSetSpec() {
    return FeatureSetSpec.newBuilder()
        .addEntities(EntitySpec.newBuilder().setName("entity1"))
        .addEntities(EntitySpec.newBuilder().setName("entity2"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }

  private FeatureSetSpec getFeatureSetSpecWithNoMaxAge() {
    return FeatureSetSpec.newBuilder()
        .addEntities(EntitySpec.newBuilder().setName("entity1"))
        .addEntities(EntitySpec.newBuilder().setName("entity2"))
        .setMaxAge(Duration.newBuilder().setSeconds(0).setNanos(0).build())
        .build();
  }

}