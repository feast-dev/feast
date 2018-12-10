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

package feast.ingestion.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import feast.ingestion.util.DateUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

public class ValuesTest {
  static class ValueTest {
    Value input;
    Value expectedValue;
    Class<? extends Throwable> expectedThrowable;

    private ValueTest() {}

    public static ValueTest of(Value input, Value expected) {
      ValueTest test = new ValueTest();
      test.input = input;
      test.expectedValue = expected;
      return test;
    }

    public static ValueTest of(Value input, Class<? extends Throwable> expected) {
      ValueTest test = new ValueTest();
      test.input = input;
      test.expectedThrowable = expected;
      return test;
    }

    void apply(Function<Value, Value> func) {
      try {
        Value output = func.apply(input);
        if (expectedThrowable != null) {
          fail("expected error");
        }
        assertEquals(expectedValue, output);
      } catch (Throwable e) {
        if (expectedThrowable == null || !expectedThrowable.isInstance(e)) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testValuesOfInt32() {
    assertEquals(123, Values.ofInt32(123).getInt32Val());
  }

  @Test
  public void testValuesOfInt64() {
    assertEquals(123, Values.ofInt64(123).getInt64Val());
  }

  @Test
  public void testValuesOfString() {
    assertEquals("123", Values.ofString("123").getStringVal());
  }

  @Test
  public void testValuesOfBytes() {
    ByteString bytes = ByteString.copyFromUtf8("123");
    Assert.assertArrayEquals(
        bytes.toByteArray(), Values.ofBytes(bytes.toByteArray()).getBytesVal().toByteArray());
  }

  @Test
  public void testValuesOfByteString() {
    ByteString bytes = ByteString.copyFromUtf8("123");
    assertEquals(bytes, Values.ofBytes(bytes).getBytesVal());
  }

  @Test
  public void testValuesOfTimestampDateTime() {
    DateTime dateTime = DateTime.now().withZone(DateTimeZone.UTC);
    Assert.assertEquals(dateTime, DateUtil.toDateTime(Values.ofTimestamp(dateTime).getTimestampVal()));
  }

  @Test
  public void testValuesOfTimestamp() {
    Timestamp timestamp = DateUtil.toTimestamp(DateTime.now());
    assertEquals(timestamp, Values.ofTimestamp(timestamp).getTimestampVal());
  }

  @Test
  public void testValuesOfBool() {
    assertTrue(Values.ofBool(true).getBoolVal());
    assertFalse(Values.ofBool(false).getBoolVal());
  }

  @Test
  public void testValuesOfDouble() {
    assertEquals(Math.PI, Values.ofDouble(Math.PI).getDoubleVal(), 0.0);
  }

  @Test
  public void testValuesOfFloat() {
    float val = Double.valueOf(Math.PI).floatValue();
    assertEquals(val, Values.ofFloat(val).getFloatVal(), 0.0);
  }

  @Test
  public void testToValueTypeFromValue() {
    assertEquals(Enum.STRING, Values.toValueType(Values.ofString("asdf")));
    assertEquals(Enum.INT64, Values.toValueType(Values.ofInt64(1234)));
    assertEquals(Enum.INT32, Values.toValueType(Values.ofInt32(1234)));
    assertEquals(Enum.TIMESTAMP, Values.toValueType(Values.ofTimestamp(DateTime.now())));
    assertEquals(Enum.BOOL, Values.toValueType(Values.ofBool(false)));
    assertEquals(Enum.BYTES, Values.toValueType(Values.ofBytes(ByteString.copyFromUtf8("abcd"))));
    assertEquals(Enum.DOUBLE, Values.toValueType(Values.ofDouble(Math.PI)));
    assertEquals(Enum.FLOAT, Values.toValueType(Values.ofFloat(1.234F)));
  }

  @Test
  public void testAsType() {}

  @Test
  public void testAsString() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Map<Value, String> assertions =
        ImmutableMap.<Value, String>builder()
            .put(Values.ofString("asdf"), "asdf")
            .put(Values.ofBool(true), "true")
            .put(Values.ofFloat(1.234F), "1.234")
            .put(Values.ofDouble(Math.PI), String.valueOf(Math.PI))
            .put(Values.ofInt32(Integer.MAX_VALUE), String.valueOf(Integer.MAX_VALUE))
            .put(Values.ofInt64(Long.MAX_VALUE), String.valueOf(Long.MAX_VALUE))
            .put(Values.ofTimestamp(datetime), String.valueOf(DateUtil.toString(datetime)))
            .put(Values.ofBytes(bytes), Base64.getEncoder().encodeToString(bytes.toByteArray()))
            .build();
    for (Value value : assertions.keySet()) {
      assertEquals(Values.ofString(assertions.get(value)), Values.asString(value));
    }
  }

  @Test
  public void testAsTimestamp() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Function<Value, Value> castFunc = Values::asTimestamp;
    ValueTest.of(Values.ofString("asdclk"), IllegalArgumentException.class).apply(castFunc);
    ValueTest.of(
            Values.ofString("2019-01-01T12:01:02.123Z"),
            Values.ofTimestamp(DateUtil.toTimestamp("2019-01-01T12:01:02.123Z")))
        .apply(castFunc);
    ValueTest.of(Values.ofInt32(Integer.MAX_VALUE), UnsupportedOperationException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(Long.MAX_VALUE), UnsupportedOperationException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofBool(true), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofFloat(1.234F), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofDouble(Math.PI), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofBytes(bytes), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofTimestamp(datetime), Values.ofTimestamp(datetime)).apply(castFunc);
  }

  @Test
  public void testAsInt64() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Function<Value, Value> castFunc = Values::asInt64;
    ValueTest.of(Values.ofString("asdclk"), NumberFormatException.class).apply(castFunc);
    ValueTest.of(
            Values.ofString(String.valueOf(Integer.MAX_VALUE)), Values.ofInt64(Integer.MAX_VALUE))
        .apply(castFunc);
    ValueTest.of(Values.ofInt32(Integer.MAX_VALUE), Values.ofInt64(Integer.MAX_VALUE))
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(Long.MAX_VALUE), Values.ofInt64(Long.MAX_VALUE)).apply(castFunc);
    ValueTest.of(Values.ofBool(true), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofFloat(1.234F), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofDouble(Math.PI), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofBytes(bytes), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofTimestamp(datetime), UnsupportedOperationException.class).apply(castFunc);
  }

  @Test
  public void testAsFloat() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Function<Value, Value> castFunc = Values::asFloat;
    ValueTest.of(Values.ofString("asdclk"), NumberFormatException.class).apply(castFunc);
    ValueTest.of(Values.ofString(String.valueOf(Float.MAX_VALUE)), Values.ofFloat(Float.MAX_VALUE))
        .apply(castFunc);
    ValueTest.of(Values.ofInt32(Integer.MAX_VALUE), Values.ofFloat(Integer.MAX_VALUE))
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(Long.MAX_VALUE), UnsupportedOperationException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofBool(true), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofFloat(Float.MAX_VALUE), Values.ofFloat(Float.MAX_VALUE)).apply(castFunc);
    ValueTest.of(Values.ofDouble(Math.PI), Values.ofFloat((float)Math.PI)).apply(castFunc);
    ValueTest.of(Values.ofBytes(bytes), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofTimestamp(datetime), UnsupportedOperationException.class).apply(castFunc);
  }

  @Test
  public void testAsBool() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Function<Value, Value> castFunc = Values::asBool;
    ValueTest.of(Values.ofString("asdclk"), Values.ofBool(false)).apply(castFunc);
    ValueTest.of(Values.ofString("True"), Values.ofBool(true)).apply(castFunc);
    ValueTest.of(Values.ofString("true"), Values.ofBool(true)).apply(castFunc);
    ValueTest.of(Values.ofInt32(Integer.MAX_VALUE), IllegalArgumentException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofInt32(0), Values.ofBool(false))
        .apply(castFunc);
    ValueTest.of(Values.ofInt32(1), Values.ofBool(true))
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(Long.MAX_VALUE), IllegalArgumentException.class)
        .apply(castFunc);

    ValueTest.of(Values.ofInt64(0), Values.ofBool(false))
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(1), Values.ofBool(true))
        .apply(castFunc);
    ValueTest.of(Values.ofBool(true), Values.ofBool(true)).apply(castFunc);

    ValueTest.of(Values.ofFloat(1.234F), IllegalArgumentException.class).apply(castFunc);
    ValueTest.of(Values.ofFloat(0), Values.ofBool(false)).apply(castFunc);
    ValueTest.of(Values.ofFloat(1), Values.ofBool(true)).apply(castFunc);

    ValueTest.of(Values.ofDouble(Math.PI), IllegalArgumentException.class).apply(castFunc);
    ValueTest.of(Values.ofDouble(0), Values.ofBool(false)).apply(castFunc);
    ValueTest.of(Values.ofDouble(1), Values.ofBool(true)).apply(castFunc);

    ValueTest.of(Values.ofBytes(bytes), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofTimestamp(datetime), UnsupportedOperationException.class).apply(castFunc);
  }

  @Test
  public void testAsInt32() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Function<Value, Value> castFunc = Values::asInt32;
    ValueTest.of(Values.ofString("asdclk"), NumberFormatException.class).apply(castFunc);
    ValueTest.of(
            Values.ofString(String.valueOf(Integer.MAX_VALUE)), Values.ofInt32(Integer.MAX_VALUE))
        .apply(castFunc);
    ValueTest.of(Values.ofInt32(Integer.MAX_VALUE), Values.ofInt32(Integer.MAX_VALUE))
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(Integer.MAX_VALUE), Values.ofInt32(Integer.MAX_VALUE)).apply(castFunc);
    ValueTest.of(Values.ofInt64(Integer.MIN_VALUE), Values.ofInt32(Integer.MIN_VALUE)).apply(castFunc);
    ValueTest.of(Values.ofInt64(Long.MAX_VALUE), IllegalArgumentException.class);
    ValueTest.of(Values.ofInt64(Long.MIN_VALUE), IllegalArgumentException.class);
    ValueTest.of(Values.ofBool(true), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofFloat(1.234F), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofDouble(Math.PI), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofBytes(bytes), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofTimestamp(datetime), UnsupportedOperationException.class).apply(castFunc);
  }

  @Test
  public void testAsDouble() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Function<Value, Value> castFunc = Values::asDouble;
    ValueTest.of(
            Values.ofString(String.valueOf(Double.MAX_VALUE)), Values.ofDouble(Double.MAX_VALUE))
        .apply(castFunc);

    ValueTest.of(Values.ofString("asdclk"), NumberFormatException.class).apply(castFunc);
    ValueTest.of(Values.ofInt32(Integer.MAX_VALUE), Values.ofDouble(Integer.MAX_VALUE))
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(Long.MAX_VALUE), Values.ofDouble(Long.MAX_VALUE)).apply(castFunc);
    ValueTest.of(Values.ofBool(true), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofFloat(Float.MAX_VALUE), Values.ofDouble(Float.MAX_VALUE)).apply(castFunc);
    ValueTest.of(Values.ofDouble(Math.PI), Values.ofDouble(Math.PI)).apply(castFunc);
    ValueTest.of(Values.ofBytes(bytes), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofTimestamp(datetime), UnsupportedOperationException.class).apply(castFunc);
  }

  @Test
  public void testAsBytes() {
    DateTime datetime = DateTime.now().withZone(DateTimeZone.UTC);
    ByteString bytes = ByteString.copyFromUtf8("asdfasdfasdf");
    Function<Value, Value> castFunc = Values::asBytes;
    ValueTest.of(
            Values.ofString(String.valueOf(Double.MAX_VALUE)), UnsupportedOperationException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofInt32(Integer.MAX_VALUE), UnsupportedOperationException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofInt64(Long.MAX_VALUE), UnsupportedOperationException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofBool(true), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofFloat(Float.MAX_VALUE), UnsupportedOperationException.class)
        .apply(castFunc);
    ValueTest.of(Values.ofDouble(Math.PI), UnsupportedOperationException.class).apply(castFunc);
    ValueTest.of(Values.ofBytes(bytes), Values.ofBytes(bytes));
    ValueTest.of(Values.ofTimestamp(datetime), UnsupportedOperationException.class).apply(castFunc);
  }
}
