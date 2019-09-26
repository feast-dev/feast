package feast.serving.service.serving;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataset;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse.FeatureDataset;
import feast.serving.service.spec.SpecService;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import java.util.List;
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
  SpecService specService;

  @Mock
  Tracer tracer;

  private RedisServingService redisServingService;
  private byte[][] redisKeyList;

  @Before
  public void setUp() {
    initMocks(this);
    FeatureSetSpec featureSetSpec = FeatureSetSpec.newBuilder()
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
    when(specService.getFeatureSetSpec("featureSet:1")).thenReturn(featureSetSpec);
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
    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSet.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .setEntityDataset(EntityDataset.newBuilder()
            .addAllEntityNames(Lists.newArrayList("entity1", "entity2"))
            .addAllEntityDatasetRows(Lists.newArrayList(
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(1), strValue("a")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build(),
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(2), strValue("b")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build()
            ))
        )
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
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFeatureDatasets(FeatureDataset.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureRows(featureRows)
            .build())
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfKeysNotPresent() {
    // some keys not present, should have empty values
    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSet.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .setEntityDataset(EntityDataset.newBuilder()
            .addAllEntityNames(Lists.newArrayList("entity1", "entity2"))
            .addAllEntityDatasetRows(Lists.newArrayList(
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(1), strValue("a")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build(),
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(2), strValue("b")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build()
            ))
        )
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
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFeatureDatasets(FeatureDataset.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureRows(featureRows)
            .build())
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfMaxAgeIsExceeded() {
    // keys present, but too stale comp. to maxAge set in request
    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSet.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .setMaxAge(Duration.newBuilder().setSeconds(10))
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .setEntityDataset(EntityDataset.newBuilder()
            .addAllEntityNames(Lists.newArrayList("entity1", "entity2"))
            .addAllEntityDatasetRows(Lists.newArrayList(
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(1), strValue("a")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build(),
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(2), strValue("b")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build()
            ))
        )
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
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    FeatureRow staleFeatureRow = FeatureRow.newBuilder()
        .setEventTimestamp(Timestamp.newBuilder()) // this value should be nulled
        .addAllFields(Lists
            .newArrayList(
                Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                Field.newBuilder().setName("feature1").build(),
                Field.newBuilder().setName("feature2").build()))
        .setFeatureSet("featureSet:1")
        .build();
    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFeatureDatasets(FeatureDataset.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addFeatureRows(featureRows.get(0))
            .addFeatureRows(staleFeatureRow)
            .build())
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfDefaultMaxAgeIsExceeded() {
    // keys present, but too stale comp. to maxAge set in featureSetSpec
    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSet.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
            .build())
        .setEntityDataset(EntityDataset.newBuilder()
            .addAllEntityNames(Lists.newArrayList("entity1", "entity2"))
            .addAllEntityDatasetRows(Lists.newArrayList(
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(1), strValue("a")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build(),
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(2), strValue("b")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build()
            ))
        )
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
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    FeatureRow staleFeatureRow = FeatureRow.newBuilder()
        .setEventTimestamp(Timestamp.newBuilder()) // this value should be nulled
        .addAllFields(Lists
            .newArrayList(
                Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                Field.newBuilder().setName("entity2").setValue(strValue("b")).build(),
                Field.newBuilder().setName("feature1").build(),
                Field.newBuilder().setName("feature2").build()))
        .setFeatureSet("featureSet:1")
        .build();
    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFeatureDatasets(FeatureDataset.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addFeatureRows(featureRows.get(0))
            .addFeatureRows(staleFeatureRow)
            .build())
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldFilterOutUndesiredRows() {
    // requested rows less than the rows available in the featureset
    GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
        .addFeatureSets(FeatureSet.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureNames(Lists.newArrayList("feature1"))
            .build())
        .setEntityDataset(EntityDataset.newBuilder()
            .addAllEntityNames(Lists.newArrayList("entity1", "entity2"))
            .addAllEntityDatasetRows(Lists.newArrayList(
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(1), strValue("a")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build(),
                EntityDatasetRow.newBuilder()
                    .addAllEntityIds(Lists.newArrayList(intValue(2), strValue("b")))
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100)).build()
            ))
        )
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
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.mget(redisKeyList)).thenReturn(featureRowBytes);
    when(tracer.buildSpan(ArgumentMatchers.any())).thenReturn(Mockito.mock(SpanBuilder.class));

    GetOnlineFeaturesResponse expected = GetOnlineFeaturesResponse.newBuilder()
        .addFeatureDatasets(FeatureDataset.newBuilder()
            .setName("featureSet")
            .setVersion(1)
            .addAllFeatureRows(featureRows.stream()
                .map(fr -> fr.toBuilder().removeFields(3).build())
                .collect(Collectors.toList()))
            .build())
        .build();
    GetOnlineFeaturesResponse actual = redisServingService.getOnlineFeatures(request);
    assertThat(actual, equalTo(expected));
  }

  private Value intValue(int val) {
    return Value.newBuilder().setInt64Val(val).build();
  }

  private Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }
}