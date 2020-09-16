/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.storage.connectors.redis.retriever;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.FeatureSetProto.EntitySpec;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.proto.storage.RedisProto.RedisKey;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.proto.types.ValueProto.Value;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.OnlineRetriever;
import feast.storage.connectors.redis.serializer.RedisKeyPrefixSerializer;
import feast.storage.connectors.redis.serializer.RedisKeySerializer;
import io.lettuce.core.KeyValue;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class RedisClusterOnlineRetrieverTest {

  @Mock StatefulRedisClusterConnection<byte[], byte[]> connection;

  @Mock RedisAdvancedClusterCommands<byte[], byte[]> syncCommands;

  RedisKeySerializer serializer = new RedisKeyPrefixSerializer("test:");

  RedisKeySerializer fallbackSerializer = new RedisKeyPrefixSerializer("");

  private List<RedisKey> redisKeys;
  private FeatureSetRequest featureSetRequest;
  private List<EntityRow> entityRows;
  private List<FeatureRow> featureRows;

  @Before
  public void setUp() {
    initMocks(this);
    when(connection.sync()).thenReturn(syncCommands);
    redisKeys =
        Lists.newArrayList(
            RedisKey.newBuilder()
                .setFeatureSet("project/featureSet")
                .addAllEntities(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("a")).build()))
                .build(),
            RedisKey.newBuilder()
                .setFeatureSet("project/featureSet")
                .addAllEntities(
                    Lists.newArrayList(
                        Field.newBuilder().setName("entity1").setValue(intValue(2)).build(),
                        Field.newBuilder().setName("entity2").setValue(strValue("b")).build()))
                .build());

    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setProject("project")
            .setName("featureSet")
            .addEntities(EntitySpec.newBuilder().setName("entity1"))
            .addEntities(EntitySpec.newBuilder().setName("entity2"))
            .addFeatures(FeatureSpec.newBuilder().setName("feature1"))
            .addFeatures(FeatureSpec.newBuilder().setName("feature2"))
            .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
            .build();

    featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(featureSetSpec)
            .addFeatureReference(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
            .addFeatureReference(
                FeatureReference.newBuilder().setName("feature2").setProject("project").build())
            .build();

    entityRows =
        ImmutableList.of(
            EntityRow.newBuilder()
                .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("entity1", intValue(1))
                .putFields("entity2", strValue("a"))
                .build(),
            EntityRow.newBuilder()
                .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("entity1", intValue(2))
                .putFields("entity2", strValue("b"))
                .build());

    featureRows =
        Lists.newArrayList(
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setValue(intValue(1)).build(),
                        Field.newBuilder().setValue(intValue(1)).build()))
                .build(),
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setValue(intValue(2)).build(),
                        Field.newBuilder().setValue(intValue(2)).build()))
                .build());
  }

  @Test
  public void shouldReturnResponseWithValuesIfKeysPresent() {
    byte[] serializedKey1 = serializer.serialize(redisKeys.get(0));
    byte[] serializedKey2 = serializer.serialize(redisKeys.get(1));

    KeyValue<byte[], byte[]> keyValue1 =
        KeyValue.from(serializedKey1, Optional.of(featureRows.get(0).toByteArray()));
    KeyValue<byte[], byte[]> keyValue2 =
        KeyValue.from(serializedKey2, Optional.of(featureRows.get(1).toByteArray()));

    List<KeyValue<byte[], byte[]>> featureRowBytes = Lists.newArrayList(keyValue1, keyValue2);

    OnlineRetriever redisClusterOnlineRetriever =
        new RedisClusterOnlineRetriever.Builder(connection, serializer)
            .withFallbackSerializer(fallbackSerializer)
            .build();
    when(syncCommands.mget(serializedKey1, serializedKey2)).thenReturn(featureRowBytes);

    List<Optional<FeatureRow>> expected =
        Lists.newArrayList(
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                    .build()),
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
                    .build()));

    List<Optional<FeatureRow>> actual =
        redisClusterOnlineRetriever.getOnlineFeatures(entityRows, featureSetRequest);
    assertThat(actual, equalTo(expected));

    // check that fallback is used only when there's something to fallback
    verify(syncCommands, never()).mget();
  }

  @Test
  public void shouldReturnNullIfKeysNotPresent() {
    byte[] serializedKey1 = serializer.serialize(redisKeys.get(0));
    byte[] serializedKey2 = serializer.serialize(redisKeys.get(1));

    KeyValue<byte[], byte[]> keyValue1 =
        KeyValue.from(serializedKey1, Optional.of(featureRows.get(0).toByteArray()));
    KeyValue<byte[], byte[]> keyValue2 = KeyValue.empty(serializedKey2);

    List<KeyValue<byte[], byte[]>> featureRowBytes = Lists.newArrayList(keyValue1, keyValue2);

    OnlineRetriever redisClusterOnlineRetriever =
        new RedisClusterOnlineRetriever.Builder(connection, serializer).build();
    when(syncCommands.mget(serializedKey1, serializedKey2)).thenReturn(featureRowBytes);

    List<Optional<FeatureRow>> expected =
        Lists.newArrayList(
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                    .build()),
            Optional.empty());

    List<Optional<FeatureRow>> actual =
        redisClusterOnlineRetriever.getOnlineFeatures(entityRows, featureSetRequest);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldUseFallbackIfAvailable() {
    byte[] serializedKey1 = serializer.serialize(redisKeys.get(0));
    byte[] serializedKey2 = serializer.serialize(redisKeys.get(1));
    byte[] fallbackSerializedKey2 = fallbackSerializer.serialize(redisKeys.get(1));

    KeyValue<byte[], byte[]> keyValue1 =
        KeyValue.from(serializedKey1, Optional.of(featureRows.get(0).toByteArray()));
    KeyValue<byte[], byte[]> keyValue2 = KeyValue.empty(serializedKey2);
    KeyValue<byte[], byte[]> fallbackKeyValue2 =
        KeyValue.from(serializedKey2, Optional.of(featureRows.get(1).toByteArray()));

    List<KeyValue<byte[], byte[]>> featureRowBytes = Lists.newArrayList(keyValue1, keyValue2);
    List<KeyValue<byte[], byte[]>> fallbackFeatureRowBytes = Lists.newArrayList(fallbackKeyValue2);

    OnlineRetriever redisClusterOnlineRetriever =
        new RedisClusterOnlineRetriever.Builder(connection, serializer)
            .withFallbackSerializer(fallbackSerializer)
            .build();

    when(syncCommands.mget(serializedKey1, serializedKey2)).thenReturn(featureRowBytes);
    when(syncCommands.mget(fallbackSerializedKey2)).thenReturn(fallbackFeatureRowBytes);

    List<Optional<FeatureRow>> expected =
        Lists.newArrayList(
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                    .build()),
            Optional.of(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
                    .build()));

    List<Optional<FeatureRow>> actual =
        redisClusterOnlineRetriever.getOnlineFeatures(entityRows, featureSetRequest);
    assertThat(actual, equalTo(expected));
  }

  private Value intValue(int val) {
    return Value.newBuilder().setInt64Val(val).build();
  }

  private Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }
}
