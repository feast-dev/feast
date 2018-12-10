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

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Timestamp;
import feast.serving.ServingAPIProto.FeatureValueList;
import feast.types.ValueProto.*;

/** Utilitu class for building {@link FeatureValueList}. */
public class FeatureValueListBuilder {

  private boolean isInitialized = false;
  private Value.ValCase type;

  private GeneratedMessageV3.Builder valueBuilder;
  private TimestampList.Builder tsBuilder;

  public boolean addValue(Value newVal, Timestamp ts) {
    initializeIfNecessary(newVal, ts);
    switch (newVal.getValCase()) {
      case BYTESVAL:
        BytesList.Builder bytesBuilder = (BytesList.Builder) valueBuilder;
        bytesBuilder.addVal(newVal.getBytesVal());
        break;
      case STRINGVAL:
        StringList.Builder stringBuilder = (StringList.Builder) valueBuilder;
        stringBuilder.addVal(newVal.getStringVal());
        break;
      case INT32VAL:
        Int32List.Builder int32Builder = (Int32List.Builder) valueBuilder;
        int32Builder.addVal(newVal.getInt32Val());
        break;
      case INT64VAL:
        Int64List.Builder int64Builder = (Int64List.Builder) valueBuilder;
        int64Builder.addVal(newVal.getInt64Val());
        break;
      case DOUBLEVAL:
        DoubleList.Builder doubleBuilder = (DoubleList.Builder) valueBuilder;
        doubleBuilder.addVal(newVal.getDoubleVal());
        break;
      case FLOATVAL:
        FloatList.Builder floatBuilder = (FloatList.Builder) valueBuilder;
        floatBuilder.addVal(newVal.getFloatVal());
        break;
      case BOOLVAL:
        BoolList.Builder boolBuilder = (BoolList.Builder) valueBuilder;
        boolBuilder.addVal(newVal.getBoolVal());
        break;
      case TIMESTAMPVAL:
        TimestampList.Builder tsListBuilder = (TimestampList.Builder) valueBuilder;
        tsListBuilder.addVal(newVal.getTimestampVal());
        break;
      default:
        throw new IllegalArgumentException("unsupported type");
    }
    if (ts != null) {
      tsBuilder.addVal(ts);
    }
    return true;
  }

  public FeatureValueList build() {
    ValueList valueList = null;
    switch (type) {
      case BYTESVAL:
        valueList =
            ValueList.newBuilder().setBytesList(((BytesList.Builder) valueBuilder).build()).build();
        break;
      case STRINGVAL:
        valueList =
            ValueList.newBuilder()
                .setStringList(((StringList.Builder) valueBuilder).build())
                .build();
        break;
      case INT32VAL:
        valueList =
            ValueList.newBuilder().setInt32List(((Int32List.Builder) valueBuilder).build()).build();
        break;
      case INT64VAL:
        valueList =
            ValueList.newBuilder().setInt64List(((Int64List.Builder) valueBuilder).build()).build();
        break;
      case DOUBLEVAL:
        valueList =
            ValueList.newBuilder()
                .setDoubleList(((DoubleList.Builder) valueBuilder).build())
                .build();
        break;
      case FLOATVAL:
        valueList =
            ValueList.newBuilder().setFloatList(((FloatList.Builder) valueBuilder).build()).build();
        break;
      case BOOLVAL:
        valueList =
            ValueList.newBuilder().setBoolList(((BoolList.Builder) valueBuilder).build()).build();
        break;
      case TIMESTAMPVAL:
        valueList =
            ValueList.newBuilder()
                .setTimestampList(((TimestampList.Builder) valueBuilder).build())
                .build();
        break;
    }

    FeatureValueList.Builder featureBuilder = FeatureValueList.newBuilder().setValueList(valueList);
    if (tsBuilder != null) {
      featureBuilder.setTimestampList(tsBuilder.build());
    }
    return featureBuilder.build();
  }

  private void initializeIfNecessary(Value val, Timestamp ts) {
    if (isInitialized) {
      return;
    }

    this.type = val.getValCase();
    this.tsBuilder = (ts != null) ? TimestampList.newBuilder() : null;
    switch (type) {
      case BYTESVAL:
        valueBuilder = BytesList.newBuilder();
        break;
      case STRINGVAL:
        valueBuilder = StringList.newBuilder();
        break;
      case INT32VAL:
        valueBuilder = Int32List.newBuilder();
        break;
      case INT64VAL:
        valueBuilder = Int64List.newBuilder();
        break;
      case DOUBLEVAL:
        valueBuilder = DoubleList.newBuilder();
        break;
      case FLOATVAL:
        valueBuilder = FloatList.newBuilder();
        break;
      case BOOLVAL:
        valueBuilder = BoolList.newBuilder();
        break;
      case TIMESTAMPVAL:
        valueBuilder = TimestampList.newBuilder();
        break;
      default:
        throw new IllegalArgumentException("unsupported type");
    }

    isInitialized = true;
  }
}
