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
package feast.storage.connectors.redis.retrieval;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.storage.RedisProto.RedisKey;
import feast.storage.api.retrieval.FeatureSetRequest;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
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

  private RedisOnlineRetriever redisOnlineRetriever;
  private byte[][] redisKeyList;

  @Before
  public void setUp() {
    initMocks(this);
    when(connection.sync()).thenReturn(syncCommands);
    redisOnlineRetriever = new RedisOnlineRetriever(connection);
    redisKeyList =
        Lists.newArrayList(
                RedisKey.newBuilder()
                    .setFeatureSet("project/featureSet:1")
                    .addAllEntities(
                        Lists.newArrayList(
                            Field.newBuilder().setName("entity1").setValue(intValue(1)).build(),
                            Field.newBuilder().setName("entity2").setValue(strValue("a")).build()))
                    .build(),
                RedisKey.newBuilder()
                    .setFeatureSet("project/featureSet:1")
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
                FeatureReference.newBuilder()
                    .setName("feature1")
                    .setVersion(1)
                    .setProject("project")
                    .build())
            .addFeatureReference(
                FeatureReference.newBuilder()
                    .setName("feature2")
                    .setVersion(1)
                    .setProject("project")
                    .build())
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

    redisOnlineRetriever = new RedisOnlineRetriever(connection);
    when(connection.sync()).thenReturn(syncCommands);
    when(syncCommands.mget(redisKeyList)).thenReturn(featureRowBytes);

    List<List<FeatureRow>> expected =
        List.of(
            Lists.newArrayList(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet:1")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                    .build(),
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet:1")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(2)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(2)).build()))
                    .build()));

    List<List<FeatureRow>> actual =
        redisOnlineRetriever.getOnlineFeatures(entityRows, List.of(featureSetRequest));
    assertThat(actual, equalTo(expected));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesIfKeysNotPresent() {
    FeatureSetRequest featureSetRequest =
        FeatureSetRequest.newBuilder()
            .setSpec(getFeatureSetSpec())
            .addFeatureReference(
                FeatureReference.newBuilder()
                    .setName("feature1")
                    .setVersion(1)
                    .setProject("project")
                    .build())
            .addFeatureReference(
                FeatureReference.newBuilder()
                    .setName("feature2")
                    .setVersion(1)
                    .setProject("project")
                    .build())
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
            .map(x -> KeyValue.from(new byte[1], Optional.of(x.toByteArray())))
            .collect(Collectors.toList());
    featureRowBytes.add(null);

    redisOnlineRetriever = new RedisOnlineRetriever(connection);
    when(connection.sync()).thenReturn(syncCommands);
    when(syncCommands.mget(redisKeyList)).thenReturn(featureRowBytes);

    List<List<FeatureRow>> expected =
        List.of(
            Lists.newArrayList(
                FeatureRow.newBuilder()
                    .setEventTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .setFeatureSet("project/featureSet:1")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").setValue(intValue(1)).build(),
                            Field.newBuilder().setName("feature2").setValue(intValue(1)).build()))
                    .build(),
                FeatureRow.newBuilder()
                    .setFeatureSet("project/featureSet:1")
                    .addAllFields(
                        Lists.newArrayList(
                            Field.newBuilder().setName("feature1").build(),
                            Field.newBuilder().setName("feature2").build()))
                    .build()));

    List<List<FeatureRow>> actual =
        redisOnlineRetriever.getOnlineFeatures(entityRows, List.of(featureSetRequest));
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
        .setVersion(1)
        .addEntities(EntitySpec.newBuilder().setName("entity1"))
        .addEntities(EntitySpec.newBuilder().setName("entity2"))
        .addFeatures(FeatureSpec.newBuilder().setName("feature1"))
        .addFeatures(FeatureSpec.newBuilder().setName("feature2"))
        .setMaxAge(Duration.newBuilder().setSeconds(30)) // default
        .build();
  }

  private FeatureSetSpec getFeatureSetSpecWithNoMaxAge() {
    return FeatureSetSpec.newBuilder()
        .setProject("project")
        .setName("featureSet")
        .setVersion(1)
        .addEntities(EntitySpec.newBuilder().setName("entity1"))
        .addEntities(EntitySpec.newBuilder().setName("entity2"))
        .addFeatures(FeatureSpec.newBuilder().setName("feature1"))
        .addFeatures(FeatureSpec.newBuilder().setName("feature2"))
        .setMaxAge(Duration.newBuilder().setSeconds(0).setNanos(0).build())
        .build();
  }
}
