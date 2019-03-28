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

package feast.store.warehouse.bigquery;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.nio.ByteBuffer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import feast.ingestion.exceptions.TypeConversionException;
import feast.ingestion.util.DateUtil;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueOrBuilder;
import feast.types.ValueProto.ValueType;

/** Builder class for taking objects received from BigQuery with their type and mapping to Values */
public class ValueBigQueryBuilder {

  public static Value valueOf(Object object, StandardSQLTypeName standardSQLTypeName) {
    return new ToValueBuilder(object, standardSQLTypeName).build();
  }

  public static ValueType.Enum feastValueTypeOf(StandardSQLTypeName standardSQLTypeName) {
    return new ToValueBuilder(null, standardSQLTypeName).getValueType();
  }

  public static Object bigQueryObjectOf(ValueOrBuilder feastValue) {
    return new ToBigQueryObject(feastValue).build();
  }

  public static class ToValueBuilder {

    @Getter private final StandardSQLTypeName standardSQLTypeName;
    @Getter private final Object bigQueryObject;

    public ToValueBuilder(Object obj, StandardSQLTypeName standardSQLTypeName) {
      this.bigQueryObject = obj;
      this.standardSQLTypeName = standardSQLTypeName;
    }

    public ValueType.Enum getValueType() {
      switch (standardSQLTypeName) {
        case BOOL:
          return ValueType.Enum.BOOL;
        case BYTES: // BYTES = java.nio.HeapByteBuffer
          return ValueType.Enum.BYTES;
        case FLOAT64:
          return ValueType.Enum.DOUBLE;
        case INT64:
          return ValueType.Enum.INT64;
        case STRUCT:
        case STRING: // STRING = org.apache.avro.util.Utf8
          return ValueType.Enum.STRING;
        case DATE: // DATE = org.apache.avro.util.Utf8 (yyyy-MM-dd)
        case DATETIME: // DATETIME = org.apache.avro.util.Utf8 (yyyy-MM-ddTHH:mm:ss)
        case TIMESTAMP: // TIMESTAMP = java.lang.Long (in microseconds)
          return ValueType.Enum.TIMESTAMP;
        case TIME: // TIME = org.apache.avro.util.Utf8 (HH:mm:ss)
        case NUMERIC: // NUMERIC = java.nio.HeapByteBuffer
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "BigQuery type conversion not implemented for %s", standardSQLTypeName));
      }
    }

    private Object getFeastObject() {
      switch (standardSQLTypeName) {
        case BOOL:
          return bigQueryObject;
        case BYTES: // BYTES = java.nio.HeapByteBuffer
          return ((ByteBuffer) bigQueryObject).array();
        case FLOAT64:
          return bigQueryObject;
        case INT64:
          return bigQueryObject;
        case STRUCT:
        case STRING: // STRING = org.apache.avro.util.Utf8
          return bigQueryObject.toString();
        case DATE: // DATE = org.apache.avro.util.Utf8 (yyyy-MM-dd)
          // This is equivalent to a Timestamp with zeroed time.
          return DateUtil.toTimestamp(
              ISODateTimeFormat.dateParser()
                  .parseDateTime(bigQueryObject.toString())
                  .withZone(DateTimeZone.UTC));
        case DATETIME: // DATETIME = org.apache.avro.util.Utf8 (yyyy-MM-ddTHH:mm:ss)
          return DateUtil.toTimestamp(
              ISODateTimeFormat.dateTimeParser()
                  .parseDateTime(bigQueryObject.toString())
                  .withZone(DateTimeZone.UTC));
        case TIMESTAMP: // TIMESTAMP = java.lang.Long (in microseconds)
          return Timestamp.newBuilder().setSeconds((long) bigQueryObject / 1000000).build();
        case TIME: // TIME = org.apache.avro.util.Utf8 (HH:mm:ss)
        case NUMERIC: // NUMERIC = java.nio.HeapByteBuffer
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "BigQuery type conversion not implemented for %s", standardSQLTypeName));
      }
    }

    public Value build() {
      Object feastObject = getFeastObject();
      if (feastObject == null) {
        return null;
      }
      Value.Builder builder = Value.newBuilder();
      try {
        switch (getValueType()) {
          case TIMESTAMP:
            return builder.setTimestampVal((Timestamp) feastObject).build();
          case STRING:
            return builder.setStringVal((String) feastObject).build();
          case INT64:
            return builder.setInt64Val((long) feastObject).build();
          case FLOAT:
            return builder.setFloatVal((float) feastObject).build();
          case BYTES:
            return builder.setBytesVal(ByteString.copyFrom((byte[]) feastObject)).build();
          case BOOL:
            return builder.setBoolVal((boolean) feastObject).build();
          case INT32:
            return builder.setInt32Val((int) feastObject).build();
          case DOUBLE:
            return builder.setDoubleVal((double) feastObject).build();
        }
      } catch (ClassCastException e) {
        throw new TypeConversionException("Could not cast bigquery type", e);
      }
      return builder.build();
    }
  }

  @AllArgsConstructor
  public static class ToBigQueryObject {

    private final ValueOrBuilder feastValue;

    public Object build() {
      switch (feastValue.getValCase()) {
        case BOOLVAL:
          return feastValue.getBoolVal();
        case FLOATVAL:
          return (double) feastValue.getFloatVal(); // all floats are 64 bit in BQ.
        case INT32VAL:
          return (long) feastValue.getInt32Val(); // all integers are 64 bit in BQ
        case INT64VAL:
          return feastValue.getInt64Val();
        case DOUBLEVAL:
          return feastValue.getDoubleVal();
        case STRINGVAL:
          return feastValue.getStringVal();
        case TIMESTAMPVAL:
          return DateUtil.toDateTime(feastValue.getTimestampVal())
              .toString(ISODateTimeFormat.dateTime());
        case BYTESVAL:
          return feastValue.getBytesVal().toByteArray();
        case VAL_NOT_SET:
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Not implemented converting Value to BigQuery value for type %s",
                  feastValue.getValCase()));
      }
    }
  }
}
