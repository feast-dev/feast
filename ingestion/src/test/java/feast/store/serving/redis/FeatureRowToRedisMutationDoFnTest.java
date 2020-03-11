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
package feast.store.serving.redis;

import static org.junit.Assert.*;

import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.storage.RedisProto.RedisKey;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import java.util.*;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FeatureRowToRedisMutationDoFnTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  private FeatureSetProto.FeatureSet fs =
      FeatureSetProto.FeatureSet.newBuilder()
          .setSpec(
              FeatureSetSpec.newBuilder()
                  .setName("feature_set")
                  .setVersion(1)
                  .addEntities(
                      EntitySpec.newBuilder()
                          .setName("entity_id_primary")
                          .setValueType(Enum.INT32)
                          .build())
                  .addEntities(
                      EntitySpec.newBuilder()
                          .setName("entity_id_secondary")
                          .setValueType(Enum.STRING)
                          .build())
                  .addFeatures(
                      FeatureSpec.newBuilder()
                          .setName("feature_1")
                          .setValueType(Enum.STRING)
                          .build())
                  .addFeatures(
                      FeatureSpec.newBuilder()
                          .setName("feature_2")
                          .setValueType(Enum.INT64)
                          .build()))
          .build();

  @Test
  public void shouldConvertRowWithDuplicateEntitiesToValidKey() {
    Map<String, FeatureSetProto.FeatureSet> featureSets = new HashMap<>();
    featureSets.put("feature_set", fs);

    FeatureRow offendingRow =
        FeatureRow.newBuilder()
            .setFeatureSet("feature_set")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(2)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_2")
                    .setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    PCollection<RedisMutation> output =
        p.apply(Create.of(Collections.singletonList(offendingRow)))
            .setCoder(ProtoCoder.of(FeatureRow.class))
            .apply(ParDo.of(new FeatureRowToRedisMutationDoFn(featureSets)));

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("feature_set")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    PAssert.that(output)
        .satisfies(
            (SerializableFunction<Iterable<RedisMutation>, Void>)
                input -> {
                  input.forEach(
                      rm -> {
                        assert (Arrays.equals(rm.getKey(), expectedKey.toByteArray()));
                        assert (Arrays.equals(rm.getValue(), expectedValue.toByteArray()));
                      });
                  return null;
                });
    p.run();
  }

  @Test
  public void shouldConvertRowWithOutOfOrderFieldsToValidKey() {
    Map<String, FeatureSetProto.FeatureSet> featureSets = new HashMap<>();
    featureSets.put("feature_set", fs);

    FeatureRow offendingRow =
        FeatureRow.newBuilder()
            .setFeatureSet("feature_set")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("feature_2")
                    .setValue(Value.newBuilder().setInt64Val(1001)))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .build();

    PCollection<RedisMutation> output =
        p.apply(Create.of(Collections.singletonList(offendingRow)))
            .setCoder(ProtoCoder.of(FeatureRow.class))
            .apply(ParDo.of(new FeatureRowToRedisMutationDoFn(featureSets)));

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("feature_set")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    List<Field> expectedFields =
        Arrays.asList(
            Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")).build(),
            Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001)).build());
    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addAllFields(expectedFields)
            .build();

    PAssert.that(output)
        .satisfies(
            (SerializableFunction<Iterable<RedisMutation>, Void>)
                input -> {
                  input.forEach(
                      rm -> {
                        assert (Arrays.equals(rm.getKey(), expectedKey.toByteArray()));
                        assert (Arrays.equals(rm.getValue(), expectedValue.toByteArray()));
                      });
                  return null;
                });
    p.run();
  }

  @Test
  public void shouldMergeDuplicateFeatureFields() {
    Map<String, FeatureSetProto.FeatureSet> featureSets = new HashMap<>();
    featureSets.put("feature_set", fs);

    FeatureRow featureRowWithDuplicatedFeatureFields =
        FeatureRow.newBuilder()
            .setFeatureSet("feature_set")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_2")
                    .setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    PCollection<RedisMutation> output =
        p.apply(Create.of(Collections.singletonList(featureRowWithDuplicatedFeatureFields)))
            .setCoder(ProtoCoder.of(FeatureRow.class))
            .apply(ParDo.of(new FeatureRowToRedisMutationDoFn(featureSets)));

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("feature_set")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt64Val(1001)))
            .build();

    PAssert.that(output)
        .satisfies(
            (SerializableFunction<Iterable<RedisMutation>, Void>)
                input -> {
                  input.forEach(
                      rm -> {
                        assert (Arrays.equals(rm.getKey(), expectedKey.toByteArray()));
                        assert (Arrays.equals(rm.getValue(), expectedValue.toByteArray()));
                      });
                  return null;
                });
    p.run();
  }

  @Test
  public void shouldPopulateMissingFeatureValuesWithDefaultInstance() {
    Map<String, FeatureSetProto.FeatureSet> featureSets = new HashMap<>();
    featureSets.put("feature_set", fs);

    FeatureRow featureRowWithDuplicatedFeatureFields =
        FeatureRow.newBuilder()
            .setFeatureSet("feature_set")
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addFields(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .addFields(
                Field.newBuilder()
                    .setName("feature_1")
                    .setValue(Value.newBuilder().setStringVal("strValue1")))
            .build();

    PCollection<RedisMutation> output =
        p.apply(Create.of(Collections.singletonList(featureRowWithDuplicatedFeatureFields)))
            .setCoder(ProtoCoder.of(FeatureRow.class))
            .apply(ParDo.of(new FeatureRowToRedisMutationDoFn(featureSets)));

    RedisKey expectedKey =
        RedisKey.newBuilder()
            .setFeatureSet("feature_set")
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_primary")
                    .setValue(Value.newBuilder().setInt32Val(1)))
            .addEntities(
                Field.newBuilder()
                    .setName("entity_id_secondary")
                    .setValue(Value.newBuilder().setStringVal("a")))
            .build();

    FeatureRow expectedValue =
        FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setStringVal("strValue1")))
            .addFields(Field.newBuilder().setValue(Value.getDefaultInstance()))
            .build();

    PAssert.that(output)
        .satisfies(
            (SerializableFunction<Iterable<RedisMutation>, Void>)
                input -> {
                  input.forEach(
                      rm -> {
                        assert (Arrays.equals(rm.getKey(), expectedKey.toByteArray()));
                        assert (Arrays.equals(rm.getValue(), expectedValue.toByteArray()));
                      });
                  return null;
                });
    p.run();
  }
}
