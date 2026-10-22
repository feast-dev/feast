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
package feast.serving.connectors;

import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;
import java.util.HashMap;
import java.util.Map;

public interface Feature {

  Map<ValueProto.ValueType.Enum, Value.ValCase> TYPE_TO_VAL_CASE =
      new HashMap<>() {
        {
          put(ValueProto.ValueType.Enum.BYTES, ValueProto.Value.ValCase.BYTES_VAL);
          put(ValueProto.ValueType.Enum.STRING, ValueProto.Value.ValCase.STRING_VAL);
          put(ValueProto.ValueType.Enum.INT32, ValueProto.Value.ValCase.INT32_VAL);
          put(ValueProto.ValueType.Enum.INT64, ValueProto.Value.ValCase.INT64_VAL);
          put(ValueProto.ValueType.Enum.DOUBLE, ValueProto.Value.ValCase.DOUBLE_VAL);
          put(ValueProto.ValueType.Enum.FLOAT, ValueProto.Value.ValCase.FLOAT_VAL);
          put(ValueProto.ValueType.Enum.BOOL, ValueProto.Value.ValCase.BOOL_VAL);
          put(ValueProto.ValueType.Enum.BYTES_LIST, ValueProto.Value.ValCase.BYTES_LIST_VAL);
          put(ValueProto.ValueType.Enum.STRING_LIST, ValueProto.Value.ValCase.STRING_LIST_VAL);
          put(ValueProto.ValueType.Enum.INT32_LIST, ValueProto.Value.ValCase.INT32_LIST_VAL);
          put(ValueProto.ValueType.Enum.INT64_LIST, ValueProto.Value.ValCase.INT64_LIST_VAL);
          put(ValueProto.ValueType.Enum.DOUBLE_LIST, ValueProto.Value.ValCase.DOUBLE_LIST_VAL);
          put(ValueProto.ValueType.Enum.FLOAT_LIST, ValueProto.Value.ValCase.FLOAT_LIST_VAL);
          put(ValueProto.ValueType.Enum.BOOL_LIST, ValueProto.Value.ValCase.BOOL_LIST_VAL);
        }
      };

  Value getFeatureValue(ValueProto.ValueType.Enum valueType);

  FeatureReferenceV2 getFeatureReference();

  Timestamp getEventTimestamp();
}
