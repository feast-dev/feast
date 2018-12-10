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

package feast.serving.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.FeatureValueList;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueList;
import feast.types.ValueProto.ValueType;
import feast.types.ValueProto.ValueType.Enum;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class FeatureBuilderTest {

  private static final Random random = new Random();

  @Test
  public void shouldBeAbleToStoreValueWithoutTimestamp() {
    for (ValueType.Enum type : ValueType.Enum.values()) {
      if (type.equals(Enum.UNKNOWN) || type.equals(Enum.UNRECOGNIZED)) {
        continue;
      }

      Value val = createRandomValue(type);

      FeatureValueListBuilder featureBuilder = new FeatureValueListBuilder();
      featureBuilder.addValue(val, null);

      FeatureValueList feature = featureBuilder.build();
      assertThat(getValueList(feature.getValueList()).get(0), equalTo(getValue(val)));
    }
  }

  @Test
  public void shouldBeAbleToStoreValueWithTimestamp() {
    for (ValueType.Enum type : ValueType.Enum.values()) {
      if (type.equals(Enum.UNKNOWN) || type.equals(Enum.UNRECOGNIZED)) {
        continue;
      }

      Value val = createRandomValue(type);

      FeatureValueListBuilder featureBuilder = new FeatureValueListBuilder();
      Timestamp ts = Timestamp.newBuilder().setSeconds(1234).build();

      featureBuilder.addValue(val, ts);

      FeatureValueList feature = featureBuilder.build();
      assertThat(getValueList(feature.getValueList()).get(0), equalTo(getValue(val)));
      assertThat(feature.getTimestampList().getVal(0), equalTo(ts));
    }
  }

  @Test
  public void shouldBeAbleToStoreMultipleValueWithTimestamp() {

    for (ValueType.Enum type : ValueType.Enum.values()) {
      if (type.equals(Enum.UNKNOWN) || type.equals(Enum.UNRECOGNIZED)) {
        continue;
      }

      FeatureValueListBuilder fb = new FeatureValueListBuilder();
      List<Value> values = new ArrayList<>();
      List<Timestamp> timestamps = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Value val = createRandomValue(type);
        Timestamp ts = Timestamp.newBuilder().setSeconds(i).build();
        fb.addValue(val, ts);

        values.add(val);
        timestamps.add(ts);
      }

      FeatureValueList feature = fb.build();
      for (int i = 0; i < 10; i++) {
        assertThat(getValueList(feature.getValueList()).get(i), equalTo(getValue(values.get(i))));
        assertThat(feature.getTimestampList().getVal(i), equalTo(timestamps.get(i)));
      }
    }
  }

  private Value createRandomValue(ValueType.Enum type) {
    Value.Builder valBuilder = Value.newBuilder();
    switch (type) {
      case INT32:
        valBuilder.setInt32Val(random.nextInt());
        break;
      case INT64:
        valBuilder.setInt64Val(random.nextLong());
        break;
      case BYTES:
        byte[] randomBytes = new byte[5];
        random.nextBytes(randomBytes);
        valBuilder.setBytesVal(ByteString.copyFrom(randomBytes));
        break;
      case STRING:
        valBuilder.setStringVal("value_" + random.nextInt());
        break;
      case BOOL:
        valBuilder.setBoolVal(true);
        break;
      case FLOAT:
        valBuilder.setFloatVal(random.nextFloat());
        break;
      case DOUBLE:
        valBuilder.setDoubleVal(random.nextDouble());
        break;
      case TIMESTAMP:
        valBuilder.setTimestampVal(Timestamps.fromSeconds(random.nextInt()));
        break;
      default:
        throw new IllegalArgumentException("unknown type: " + type);
    }

    return valBuilder.build();
  }

  private Object getValue(Value value) {
    switch (value.getValCase()) {
      case FLOATVAL:
        return value.getFloatVal();
      case BOOLVAL:
        return value.getBoolVal();
      case BYTESVAL:
        return value.getBytesVal();
      case INT32VAL:
        return value.getInt32Val();
      case INT64VAL:
        return value.getInt64Val();
      case STRINGVAL:
        return value.getStringVal();
      case DOUBLEVAL:
        return value.getDoubleVal();
      case TIMESTAMPVAL:
        return value.getTimestampVal();
      default:
        throw new IllegalArgumentException("Unknown type");
    }
  }

  private List<?> getValueList(ValueList valueList) {
    switch (valueList.getValueListCase()) {
      case BOOLLIST:
        return valueList.getBoolList().getValList();
      case BYTESLIST:
        return valueList.getBytesList().getValList();
      case FLOATLIST:
        return valueList.getFloatList().getValList();
      case INT32LIST:
        return valueList.getInt32List().getValList();
      case INT64LIST:
        return valueList.getInt64List().getValList();
      case DOUBLELIST:
        return valueList.getDoubleList().getValList();
      case STRINGLIST:
        return valueList.getStringList().getValList();
      case TIMESTAMPLIST:
        return valueList.getTimestampList().getValList();
      default:
        throw new IllegalArgumentException("Unknown type");
    }
  }
}
