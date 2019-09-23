// /*
//  * Copyright 2018 The Feast Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     https://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  *
//  */
//
// package feast.ingestion.model;
//
// import com.google.protobuf.ByteString;
// import com.google.protobuf.Timestamp;
// import feast.ingestion.exceptions.TypeConversionException;
// import feast.ingestion.util.DateUtil;
// import feast.types.ValueProto.Value;
// import feast.types.ValueProto.Value.ValCase;
// import feast.types.ValueProto.ValueType;
// import feast.types.ValueProto.ValueType.Enum;
// import java.util.Base64;
// import java.util.HashMap;
// import java.util.Map;
// import lombok.AllArgsConstructor;
// import org.apache.beam.sdk.transforms.SerializableFunction;
// import org.joda.time.DateTime;
//
// public class Values {
//
//   private static final Map<ValCase, SerializableFunction<Value, Object>> valCaseToObjectFuncMap =
//       new HashMap<>();
//   private static final Map<ValueType.Enum, ValCase> valueTypeToValCaseMap = new HashMap<>();
//   private static final Map<ValCase, ValueType.Enum> valCaseToValueTypeMap = new HashMap<>();
//
//   private static final Value TRUE_BOOL_VAL = Value.newBuilder().setBoolVal(true).build();
//   private static final Value FALSE_BOOL_VAL = Value.newBuilder().setBoolVal(false).build();
//
//   static {
//     Wrapper[] wrappers =
//         new Wrapper[]{
//             new Wrapper(Enum.BYTES, ValCase.BYTES_VAL, Value::getBytesVal),
//             new Wrapper(Enum.STRING, ValCase.STRING_VAL, Value::getStringVal),
//             new Wrapper(Enum.INT32, ValCase.INT32_VAL, Value::getInt32Val),
//             new Wrapper(Enum.INT64, ValCase.INT64_VAL, Value::getInt64Val),
//             new Wrapper(Enum.DOUBLE, ValCase.DOUBLE_VAL, Value::getDoubleVal),
//             new Wrapper(Enum.FLOAT, ValCase.FLOAT_VAL, Value::getFloatVal),
//             new Wrapper(Enum.BOOL, ValCase.BOOL_VAL, Value::getBoolVal),
//             // new Wrapper(Enum.TIMESTAMP, ValCase.TIMESTAMPVAL, Value::getTimestampVal)
//         };
//     for (Wrapper wrapper : wrappers) {
//       valueTypeToValCaseMap.put(wrapper.valueType, wrapper.valCase);
//       valCaseToValueTypeMap.put(wrapper.valCase, wrapper.valueType);
//       valCaseToObjectFuncMap.put(wrapper.valCase, wrapper.toObject);
//     }
//   }
//   private Values() {}
//
//   public static Value ofInt64(long val) {
//     return Value.newBuilder().setInt64Val(val).build();
//   }
//
//   public static Value ofInt32(int val) {
//     return Value.newBuilder().setInt32Val(val).build();
//   }
//
//   public static Value ofFloat(float val) {
//     return Value.newBuilder().setFloatVal(val).build();
//   }
//
//   public static Value ofDouble(double val) {
//     return Value.newBuilder().setDoubleVal(val).build();
//   }
//
//   public static Value ofString(String val) {
//     return Value.newBuilder().setStringVal(val).build();
//   }
//
//   public static Value ofBool(boolean val) {
//     return (val) ? TRUE_BOOL_VAL : FALSE_BOOL_VAL;
//   }
//
//   // public static Value ofTimestamp(Timestamp val) {
//   //   return Value.newBuilder().setTimestampVal(val).build();
//   // }
//
//   // public static Value ofTimestamp(DateTime val) {
//   //   return ofTimestamp(DateUtil.toTimestamp(val));
//   // }
//
//   public static Value ofBytes(ByteString val) {
//     return Value.newBuilder().setBytesVal(val).build();
//   }
//
//   public static Value ofBytes(byte[] val) {
//     return ofBytes(ByteString.copyFrom(val));
//   }
//
//   public static ValueType.Enum toValueType(Value value) {
//     return valCaseToValueTypeMap.getOrDefault(value.getValCase(), Enum.UNKNOWN);
//   }
//
//   private static ValueType.Enum toValueType(ValCase valCase) {
//     return valCaseToValueTypeMap.getOrDefault(valCase, Enum.UNKNOWN);
//   }
//
//   private static Object toObject(Value value) {
//     return valCaseToObjectFuncMap.get(value.getValCase()).apply(value);
//   }
//
//   public static Value asType(Value value, ValueType.Enum valueType) throws TypeConversionException {
//     return asType(value, valueTypeToValCaseMap.get(valueType));
//   }
//
//   private static Value asType(Value value, ValCase valCase) throws TypeConversionException {
//     if (value.getValCase() == valCase) {
//       return value;
//     }
//     try {
//       switch (valCase) {
//         case BYTES_VAL:
//           return asBytes(value);
//         case STRING_VAL:
//           return asString(value);
//         case INT32_VAL:
//           return asInt32(value);
//         case INT64_VAL:
//           return asInt64(value);
//         case DOUBLE_VAL:
//           return asDouble(value);
//         case FLOAT_VAL:
//           return asFloat(value);
//         case BOOL_VAL:
//           return asBool(value);
//         // case TIMESTAMPVAL:
//         //   return asTimestamp(value);
//       }
//       throw new TypeConversionException();
//     } catch (UnsupportedOperationException e) {
//       String message =
//           String.format(
//               "Converting not supported from type %s to type %s type",
//               toValueType(value), toValueType(valCase));
//       throw new TypeConversionException(message, e);
//     } catch (Throwable e) {
//       String message =
//           String.format(
//               "Exception while converting from type %s to type %s type",
//               toValueType(value), toValueType(valCase));
//       throw new TypeConversionException(message, e);
//     }
//   }
//
//   public static Value asString(Value value) {
//     switch (value.getValCase()) {
//       case STRING_VAL:
//         return value;
//       // case TIMESTAMP_VAL:
//       //   return ofString(DateUtil.toString(value.getTimestampVal()));
//       case BYTES_VAL:
//         return ofString(Base64.getEncoder().encodeToString(value.getBytesVal().toByteArray()));
//       default:
//         return ofString(toObject(value).toString());
//     }
//   }
//
//   public static Value asTimestamp(Value value) {
//     switch (value.getValCase()) {
//       // case TIMESTAMPVAL:
//       //   return value;
//       // case STRINGVAL:
//       //   return ofTimestamp(DateUtil.toTimestamp(value.getStringVal()));
//       default:
//         throw new UnsupportedOperationException();
//     }
//   }
//
//   public static Value asInt64(Value value) {
//     switch (value.getValCase()) {
//       case INT64_VAL:
//         return value;
//       case INT32_VAL:
//         return ofInt64(value.getInt32Val());
//       case STRING_VAL:
//         return ofInt64(Long.valueOf(value.getStringVal()));
//       default:
//         throw new UnsupportedOperationException();
//     }
//   }
//
//   public static Value asFloat(Value value) {
//     switch (value.getValCase()) {
//       case FLOAT_VAL:
//         return value;
//       case STRING_VAL:
//         return ofFloat(Float.valueOf(value.getStringVal()));
//       case INT32_VAL:
//         return ofFloat((float) value.getInt32Val());
//       case DOUBLE_VAL:
//         return ofFloat((float) value.getDoubleVal());
//       default:
//         throw new UnsupportedOperationException();
//     }
//   }
//
//   public static Value asBool(Value value) {
//     switch (value.getValCase()) {
//       case BOOL_VAL:
//         return value;
//       case STRING_VAL:
//         return ofBool(Boolean.valueOf(value.getStringVal()));
//       case INT32_VAL:
//         int int32Val = value.getInt32Val();
//         boolean isInt32One = int32Val == 1;
//         boolean isInt32Zero = int32Val == 0;
//         if (!isInt32One && !isInt32Zero) {
//           throw new IllegalArgumentException(
//               "Only int32 value of 0 or 1 can be converted to boolean, got: " + int32Val);
//         }
//         return ofBool(isInt32One);
//       case INT64_VAL:
//         long int64Val = value.getInt64Val();
//         boolean isInt64One = int64Val == 1;
//         boolean isInt64Zero = int64Val == 0;
//         if (!isInt64One && !isInt64Zero) {
//           throw new IllegalArgumentException(
//               "Only int64 value of 0 or 1 can be converted to boolean, got: " + int64Val);
//         }
//         return ofBool(isInt64One);
//       case FLOAT_VAL:
//         float floatVal = value.getFloatVal();
//         boolean isFloatOne = Float.compare(floatVal, 1) == 0;
//         boolean isFloatZero = Float.compare(floatVal, 0) == 0;
//         if (!isFloatOne && !isFloatZero) {
//           throw new IllegalArgumentException(
//               "Only float value of 0.0 or 1.0 can be converted to boolean, got: " + floatVal);
//         }
//         return ofBool(isFloatOne);
//       case DOUBLE_VAL:
//         double doubleVal = value.getDoubleVal();
//         boolean isDoubleOne = Double.compare(doubleVal, 1) == 0;
//         boolean isDoubleZero = Double.compare(doubleVal, 0) == 0;
//         if (!isDoubleOne && !isDoubleZero) {
//           throw new IllegalArgumentException(
//               "Only double value of 0.0 or 1.0 can be converted to boolean, got: " + doubleVal);
//         }
//         return ofBool(isDoubleOne);
//       default:
//         throw new UnsupportedOperationException();
//     }
//   }
//
//   public static Value asInt32(Value value) {
//     switch (value.getValCase()) {
//       case INT32_VAL:
//         return value;
//       case STRING_VAL:
//         return ofInt32(Integer.valueOf(value.getStringVal()));
//       case INT64_VAL:
//         long longValue = value.getInt64Val();
//         if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
//           throw new IllegalArgumentException(
//               "The int64 value can't be casted to int32: " + longValue);
//         }
//         return ofInt32((int) longValue);
//       default:
//         throw new UnsupportedOperationException();
//     }
//   }
//
//   public static Value asDouble(Value value) {
//     switch (value.getValCase()) {
//       case DOUBLE_VAL:
//         return value;
//       case FLOAT_VAL:
//         return ofDouble(value.getFloatVal());
//       case INT32_VAL:
//         return ofDouble(value.getInt32Val());
//       case INT64_VAL:
//         return ofDouble(value.getInt64Val());
//       case STRING_VAL:
//         return ofDouble(Double.valueOf(value.getStringVal()));
//       default:
//         throw new UnsupportedOperationException();
//     }
//   }
//
//   public static Value asBytes(Value value) {
//     switch (value.getValCase()) {
//       case BYTES_VAL:
//         return value;
//       default:
//         throw new UnsupportedOperationException();
//     }
//   }
//
//   @AllArgsConstructor
//   static class Wrapper {
//
//     ValueType.Enum valueType;
//     ValCase valCase;
//     SerializableFunction<Value, Object> toObject;
//   }
// }
