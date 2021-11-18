/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.storage.api.retriever;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

public class AvroFeature implements Feature {
  private final ServingAPIProto.FeatureReferenceV2 featureReference;

  private final Timestamp eventTimestamp;

  private final Object featureValue;

  public AvroFeature(
      ServingAPIProto.FeatureReferenceV2 featureReference,
      Timestamp eventTimestamp,
      Object featureValue) {
    this.featureReference = featureReference;
    this.eventTimestamp = eventTimestamp;
    this.featureValue = featureValue;
  }

  /**
   * Casts feature value of Object type based on Feast valueType. Empty object i.e new Object() is
   * interpreted as VAL_NOT_SET Feast valueType.
   *
   * @param valueType Feast valueType of feature as specified in FeatureSpec
   * @return ValueProto.Value representation of feature
   */
  @Override
  public ValueProto.Value getFeatureValue(ValueProto.ValueType.Enum valueType) {
    ValueProto.Value finalValue;

    try {
      switch (valueType) {
        case STRING:
          finalValue =
              ValueProto.Value.newBuilder().setStringVal(((Utf8) featureValue).toString()).build();
          break;
        case INT32:
          finalValue = ValueProto.Value.newBuilder().setInt32Val((Integer) featureValue).build();
          break;
        case INT64:
          finalValue = ValueProto.Value.newBuilder().setInt64Val((Long) featureValue).build();
          break;
        case DOUBLE:
          finalValue = ValueProto.Value.newBuilder().setDoubleVal((Double) featureValue).build();
          break;
        case FLOAT:
          finalValue = ValueProto.Value.newBuilder().setFloatVal((Float) featureValue).build();
          break;
        case BYTES:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setBytesVal(ByteString.copyFrom(((ByteBuffer) featureValue).array()))
                  .build();
          break;
        case BOOL:
          finalValue = ValueProto.Value.newBuilder().setBoolVal((Boolean) featureValue).build();
          break;
        case STRING_LIST:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setStringListVal(
                      ValueProto.StringList.newBuilder()
                          .addAllVal(
                              ((GenericData.Array<Utf8>) featureValue)
                                  .stream().map(Utf8::toString).collect(Collectors.toList()))
                          .build())
                  .build();
          break;
        case INT64_LIST:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setInt64ListVal(
                      ValueProto.Int64List.newBuilder()
                          .addAllVal(((GenericData.Array<Long>) featureValue))
                          .build())
                  .build();
          break;
        case INT32_LIST:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setInt32ListVal(
                      ValueProto.Int32List.newBuilder()
                          .addAllVal(((GenericData.Array<Integer>) featureValue))
                          .build())
                  .build();
          break;
        case FLOAT_LIST:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setFloatListVal(
                      ValueProto.FloatList.newBuilder()
                          .addAllVal(((GenericData.Array<Float>) featureValue))
                          .build())
                  .build();
          break;
        case DOUBLE_LIST:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setDoubleListVal(
                      ValueProto.DoubleList.newBuilder()
                          .addAllVal(((GenericData.Array<Double>) featureValue))
                          .build())
                  .build();
          break;
        case BOOL_LIST:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setBoolListVal(
                      ValueProto.BoolList.newBuilder()
                          .addAllVal(((GenericData.Array<Boolean>) featureValue))
                          .build())
                  .build();
          break;
        case BYTES_LIST:
          finalValue =
              ValueProto.Value.newBuilder()
                  .setBytesListVal(
                      ValueProto.BytesList.newBuilder()
                          .addAllVal(
                              ((GenericData.Array<ByteBuffer>) featureValue)
                                  .stream()
                                      .map(byteBuffer -> ByteString.copyFrom(byteBuffer.array()))
                                      .collect(Collectors.toList()))
                          .build())
                  .build();
          break;
        default:
          throw new RuntimeException("FeatureType is not supported");
      }
    } catch (ClassCastException e) {
      // Feature type has changed
      finalValue = ValueProto.Value.newBuilder().build();
    }

    return finalValue;
  }

  @Override
  public ServingAPIProto.FeatureReferenceV2 getFeatureReference() {
    return this.featureReference;
  }

  @Override
  public Timestamp getEventTimestamp() {
    return this.eventTimestamp;
  }
}
