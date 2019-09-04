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
// package feast.store.warehouse.bigquery;
//
// import static junit.framework.TestCase.assertEquals;
// import static junit.framework.TestCase.assertTrue;
// import static org.hamcrest.MatcherAssert.assertThat;
// import static org.hamcrest.Matchers.equalTo;
// import static org.hamcrest.Matchers.is;
//
// import com.google.protobuf.ByteString;
// import com.google.protobuf.Timestamp;
// import java.nio.charset.StandardCharsets;
// import org.junit.Rule;
// import org.junit.Test;
// import org.junit.rules.TemporaryFolder;
// import feast.ingestion.util.DateUtil;
// import feast.types.ValueProto.Value;
//
// public class ValueBigQueryBuilderTest {
//
//   @Rule public TemporaryFolder folder = new TemporaryFolder();
//
//   @Test
//   public void testINT64IsConvertedToLong() {
//     Value value = Value.newBuilder().setInt64Val(1234L).build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(Long.class.isInstance(actual));
//     assertEquals(1234L, actual);
//   }
//
//   @Test
//   public void testINT32IsConvertedToLong() {
//     Value value = Value.newBuilder().setInt32Val(1234).build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(Long.class.isInstance(actual));
//     assertEquals(1234L, actual);
//   }
//
//   @Test
//   public void testSTRINGIsConvertedToString() {
//     Value value = Value.newBuilder().setStringVal("abcd").build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(String.class.isInstance(actual));
//     assertEquals("abcd", actual);
//   }
//
//   @Test
//   public void testTIMESTAMPIsConvertedToString() {
//     Timestamp timestamp = DateUtil.toTimestamp("2017-01-01T13:14:15.123Z");
//     Value value = Value.newBuilder().setTimestampVal(timestamp).build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(String.class.isInstance(actual));
//     assertEquals("2017-01-01T13:14:15.123Z", actual);
//   }
//
//   @Test
//   public void testBOOLIsConvertedToString() {
//     Value value = Value.newBuilder().setBoolVal(true).build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(Boolean.class.isInstance(actual));
//     assertEquals(true, actual);
//   }
//
//   @Test
//   public void testDOUBLEIsConvertedToDouble() {
//     Value value = Value.newBuilder().setDoubleVal(1.123456767890123456789).build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(Double.class.isInstance(actual));
//     assertEquals(1.123456767890123456789, actual);
//   }
//
//   @Test
//   public void testFLOATIsConvertedToDouble() {
//     Value value = Value.newBuilder().setFloatVal(1.123456767890123456789f).build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(Double.class.isInstance(actual));
//     assertEquals(1.1234567f, ((Double) actual).floatValue());
//   }
//
//   @Test
//   public void testBYTESIsConvertedToByteBuffer() {
//     Value value =
//         Value.newBuilder()
//             .setBytesVal(ByteString.copyFrom("to bytes!".getBytes(StandardCharsets.UTF_8)))
//             .build();
//     Object actual = ValueBigQueryBuilder.bigQueryObjectOf(value);
//     assertTrue(byte[].class.isInstance(actual));
//     assertThat("to bytes!".getBytes(StandardCharsets.UTF_8), is(equalTo(actual)));
//   }
// }
