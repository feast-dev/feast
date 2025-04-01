/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package dev.feast;

import com.google.protobuf.ByteString;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
public class RequestUtil {

  /**
   * Create feature references protos from given string feature reference.
   *
   * @param featureRefStrings to create Feature Reference protos from
   * @return List of parsed {@link FeatureReferenceV2} protos
   */
  public static List<FeatureReferenceV2> createFeatureRefs(List<String> featureRefStrings) {
    if (featureRefStrings == null) {
      throw new IllegalArgumentException("FeatureReferences cannot be null");
    }

    List<FeatureReferenceV2> featureRefs =
        featureRefStrings.stream().map(RequestUtil::parseFeatureRef).collect(Collectors.toList());

    return featureRefs;
  }

  /**
   * Parse a feature reference proto builder from the given featureRefString
   *
   * @param featureRefString string feature reference to parse from.
   * @return a parsed {@link FeatureReferenceV2}
   */
  public static FeatureReferenceV2 parseFeatureRef(String featureRefString) {
    featureRefString = featureRefString.trim();
    if (featureRefString.isEmpty()) {
      throw new IllegalArgumentException("Cannot parse a empty feature reference");
    }
    if (featureRefString.contains("/")) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported feature reference: Specifying project in string"
                  + " Feature References is not longer supported: %s",
              featureRefString));
    }
    if (!featureRefString.contains(":")) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported feature reference: %s - FeatureTable name and Feature name should be provided in string"
                  + " Feature References, in <featureTableName>:<featureName> format.",
              featureRefString));
    }

    String[] featureReferenceParts = featureRefString.split(":");
    FeatureReferenceV2 featureRef =
        FeatureReferenceV2.newBuilder()
            .setFeatureViewName(featureReferenceParts[0])
            .setFeatureName(featureReferenceParts[1])
            .build();

    return featureRef;
  }

  public static Value objectToValue(Object value) {
    if (value == null) {
      return Value.newBuilder().setNullVal(ValueProto.Null.NULL).build();
    }
    switch (value.getClass().getCanonicalName()) {
      case "java.lang.Integer":
        return Value.newBuilder().setInt32Val((int) value).build();
      case "java.lang.Long":
        return Value.newBuilder().setInt64Val((long) value).build();
      case "java.lang.Float":
        return Value.newBuilder().setFloatVal((float) value).build();
      case "java.lang.Double":
        return Value.newBuilder().setDoubleVal((double) value).build();
      case "java.lang.String":
        return Value.newBuilder().setStringVal((String) value).build();
      case "byte[]":
        return Value.newBuilder().setBytesVal(ByteString.copyFrom((byte[]) value)).build();
      case "java.lang.Boolean":
        return Value.newBuilder().setBoolVal((boolean) value).build();
      case "feast.proto.types.ValueProto.Value":
        return (Value) value;
      case "java.util.Arrays.ArrayList":
        if (((List<?>) value).isEmpty()) {
          throw new IllegalArgumentException("Unsupported empty list type");
        }

        try {
          switch (((List<?>) value).get(0).getClass().getCanonicalName()) {
            case "java.lang.Integer":
              return Value.newBuilder()
                  .setInt32ListVal(
                      ValueProto.Int32List.newBuilder().addAllVal((List<Integer>) value).build())
                  .build();
            case "java.lang.Long":
              return Value.newBuilder()
                  .setInt64ListVal(
                      ValueProto.Int64List.newBuilder().addAllVal((List<Long>) value).build())
                  .build();
            case "java.lang.Float":
              return Value.newBuilder()
                  .setFloatListVal(
                      ValueProto.FloatList.newBuilder().addAllVal((List<Float>) value).build())
                  .build();
            case "java.lang.Double":
              return Value.newBuilder()
                  .setDoubleListVal(
                      ValueProto.DoubleList.newBuilder().addAllVal((List<Double>) value).build())
                  .build();
            case "java.lang.String":
              return Value.newBuilder()
                  .setStringListVal(
                      ValueProto.StringList.newBuilder().addAllVal((List<String>) value).build())
                  .build();
            case "byte[]":
              List<ByteString> byteList =
                  ((List<byte[]>) value)
                      .stream().map(ByteString::copyFrom).collect(Collectors.toList());
              return Value.newBuilder()
                  .setBytesListVal(ValueProto.BytesList.newBuilder().addAllVal(byteList).build())
                  .build();
            case "java.lang.Boolean":
              return Value.newBuilder()
                  .setBoolListVal(
                      ValueProto.BoolList.newBuilder().addAllVal((List<Boolean>) value).build())
                  .build();
            default:
              throw new IllegalArgumentException(
                  String.format(
                      "Unsupported list type: %s",
                      ((List<?>) value).get(0).getClass().getSimpleName()));
          }
        } catch (ClassCastException e) {
          throw new IllegalArgumentException(
              String.format("Unknown list type, error during casting: %s", e.getMessage()));
        }
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported type: %s", value.getClass().getSimpleName()));
    }
  }
}
