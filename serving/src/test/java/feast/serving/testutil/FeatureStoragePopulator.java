/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.serving.testutil;

import com.google.protobuf.ByteString;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.types.FieldProto.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.BoolList;
import feast.types.ValueProto.BytesList;
import feast.types.ValueProto.DoubleList;
import feast.types.ValueProto.FloatList;
import feast.types.ValueProto.Int32List;
import feast.types.ValueProto.Int64List;
import feast.types.ValueProto.StringList;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType;
import java.util.List;

public abstract class FeatureStoragePopulator {

  /**
   * Populate feature storage with fake data.
   */
  public abstract void populate(List<Field> fields, FeatureSetSpec featureSetSpec,
      FeatureRow featureRow);

  protected Value createValue(ValueType.Enum valType, Object value) {
    switch (valType) {
      case BYTES:
        return Value.newBuilder().setBytesVal((ByteString) value).build();
      case STRING:
        return Value.newBuilder().setStringVal((String) value).build();
      case INT32:
        return Value.newBuilder().setInt32Val((Integer) value).build();
      case INT64:
        return Value.newBuilder().setInt64Val((Long) value).build();
      case DOUBLE:
        return Value.newBuilder().setDoubleVal((Double) value).build();
      case FLOAT:
        return Value.newBuilder().setFloatVal((Float) value).build();
      case BOOL:
        return Value.newBuilder().setBoolVal((Boolean) value).build();
      case BYTES_LIST:
        return Value.newBuilder().setBytesListVal((BytesList) value).build();
      case STRING_LIST:
        return Value.newBuilder().setStringListVal((StringList) value).build();
      case INT32_LIST:
        return Value.newBuilder().setInt32ListVal((Int32List) value).build();
      case INT64_LIST:
        return Value.newBuilder().setInt64ListVal((Int64List) value).build();
      case DOUBLE_LIST:
        return Value.newBuilder().setDoubleListVal((DoubleList) value).build();
      case FLOAT_LIST:
        return Value.newBuilder().setFloatListVal((FloatList) value).build();
      case BOOL_LIST:
        return Value.newBuilder().setBoolListVal((BoolList) value).build();
      default:
        throw new IllegalArgumentException("not yet supported");
    }
  }

}
