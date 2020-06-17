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
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.AbstractMessageLite;
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
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class RedisOnlineRetrieverTest {

  @Mock StatefulRedisConnection<byte[], byte[]> connection;

  @Mock RedisCommands<byte[], byte[]> syncCommands;

  private OnlineRetriever redisOnlineRetriever;
  private byte[][] redisKeyList;

  @Before
  public void setUp() {
    initMocks(this);
    when(connection.sync()).thenReturn(syncCommands);
    redisOnlineRetriever = RedisOnlineRetriever.create(connection);
    redisKeyList =
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
                    .build())
            .stream()
            .map(AbstractMessageLite::toByteArray)
            .collect(Collectors.toList())
            .toArray(new byte[0][0]);
  }

  @Test
  public void shouldReturnResponseWithValuesIfKeysPresent() {
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(getFeatureSetSpec())
            .addFeatureReference(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
            .addFeatureReference(
                FeatureReference.newBuilder().setName("feature2").setProject("project").build())
            .build();
    List<EntityRow> entityRows =
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

    List<FeatureRow> featureRows =
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

    List<KeyValue<byte[], byte[]>> featureRowBytes =
        featureRows.stream()
            .map(x -> KeyValue.from(new byte[1], Optional.of(x.toByteArray())))
            .collect(Collectors.toList());

    redisOnlineRetriever = RedisOnlineRetriever.create(connection);
    when(connection.sync()).thenReturn(syncCommands);
    when(syncCommands.mget(redisKeyList)).thenReturn(featureRowBytes);

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
        redisOnlineRetriever.getOnlineFeatures(entityRows, featureSetRequest);
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnNullIfKeysNotPresent() {
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(getFeatureSetSpec())
            .addFeatureReference(
                FeatureReference.newBuilder().setName("feature1").setProject("project").build())
            .addFeatureReference(
                FeatureReference.newBuilder().setName("feature2").setProject("project").build())
            .build();
    List<EntityRow> entityRows =
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

    List<FeatureRow> featureRows =
        Lists.newArrayList(
            FeatureRow.newBuilder()
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                .addAllFields(
                    Lists.newArrayList(
                        Field.newBuilder().setValue(intValue(1)).build(),
                        Field.newBuilder().setValue(intValue(1)).build()))
                .build());

    List<KeyValue<byte[], byte[]>> featureRowBytes =
        featureRows.stream()
            .map(row -> KeyValue.from(new byte[1], Optional.of(row.toByteArray())))
            .collect(Collectors.toList());
    featureRowBytes.add(null);

    redisOnlineRetriever = RedisOnlineRetriever.create(connection);
    when(connection.sync()).thenReturn(syncCommands);
    when(syncCommands.mget(redisKeyList)).thenReturn(featureRowBytes);

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
        redisOnlineRetriever.getOnlineFeatures(entityRows, featureSetRequest);
    assertThat(actual, equalTo(expected));
  }

  private Value intValue(int val) {
    return Value.newBuilder().setInt64Val(val).build();
  }

  private Value strValue(String val) {
    return Value.newBuilder().setStringVal(val).build();
  }

  private FeatureSetSpec getFeatureSetSpec() {
    return FeatureSetSpec.newBuilder()
        .setProject("project")
        .setName("featureSet")
        .addEntities(EntitySpec.newBuilder().setName("entity1"))
        .addEntities(EntitySpec.newBuilder().setName("entity2"))
        .addFeatures(FeatureSpec.newBuilder().setName("feature1"))
        .addFeatures(FeatureSpec.newBuilder().setName("feature2"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }
}
