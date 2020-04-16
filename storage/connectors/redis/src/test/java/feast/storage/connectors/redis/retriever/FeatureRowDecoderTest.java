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

import static org.junit.Assert.*;

import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.types.FeatureRowProto;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType;
import java.util.Collections;
import org.junit.Test;

public class FeatureRowDecoderTest {

  private FeatureSetProto.EntitySpec entity =
      FeatureSetProto.EntitySpec.newBuilder().setName("entity1").build();

  private FeatureSetSpec spec =
      FeatureSetSpec.newBuilder()
          .addAllEntities(Collections.singletonList(entity))
          .addFeatures(
              FeatureSetProto.FeatureSpec.newBuilder()
                  .setName("feature1")
                  .setValueType(ValueType.Enum.FLOAT))
          .addFeatures(
              FeatureSetProto.FeatureSpec.newBuilder()
                  .setName("feature2")
                  .setValueType(ValueType.Enum.INT32))
          .setName("feature_set_name")
          .build();

  @Test
  public void featureRowWithFieldNamesIsNotConsideredAsEncoded() {

    FeatureRowDecoder decoder = new FeatureRowDecoder("feature_set_ref", spec);
    FeatureRowProto.FeatureRow nonEncodedFeatureRow =
        FeatureRowProto.FeatureRow.newBuilder()
            .setFeatureSet("feature_set_ref")
            .setEventTimestamp(Timestamp.newBuilder().setNanos(1000))
            .addFields(
                Field.newBuilder().setName("feature1").setValue(Value.newBuilder().setInt32Val(2)))
            .addFields(
                Field.newBuilder()
                    .setName("feature2")
                    .setValue(Value.newBuilder().setFloatVal(1.0f)))
            .build();
    assertFalse(decoder.isEncoded(nonEncodedFeatureRow));
  }

  @Test
  public void encodingIsInvalidIfNumberOfFeaturesInSpecDiffersFromFeatureRow() {

    FeatureRowDecoder decoder = new FeatureRowDecoder("feature_set_ref", spec);

    FeatureRowProto.FeatureRow encodedFeatureRow =
        FeatureRowProto.FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setNanos(1000))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt32Val(2)))
            .build();

    assertFalse(decoder.isEncodingValid(encodedFeatureRow));
  }

  @Test
  public void shouldDecodeValidEncodedFeatureRow() {

    FeatureRowDecoder decoder = new FeatureRowDecoder("feature_set_ref", spec);

    FeatureRowProto.FeatureRow encodedFeatureRow =
        FeatureRowProto.FeatureRow.newBuilder()
            .setEventTimestamp(Timestamp.newBuilder().setNanos(1000))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setInt32Val(2)))
            .addFields(Field.newBuilder().setValue(Value.newBuilder().setFloatVal(1.0f)))
            .build();

    FeatureRowProto.FeatureRow expectedFeatureRow =
        FeatureRowProto.FeatureRow.newBuilder()
            .setFeatureSet("feature_set_ref")
            .setEventTimestamp(Timestamp.newBuilder().setNanos(1000))
            .addFields(
                Field.newBuilder().setName("feature1").setValue(Value.newBuilder().setInt32Val(2)))
            .addFields(
                Field.newBuilder()
                    .setName("feature2")
                    .setValue(Value.newBuilder().setFloatVal(1.0f)))
            .build();

    assertTrue(decoder.isEncoded(encodedFeatureRow));
    assertTrue(decoder.isEncodingValid(encodedFeatureRow));
    assertEquals(expectedFeatureRow, decoder.decode(encodedFeatureRow));
  }
}
