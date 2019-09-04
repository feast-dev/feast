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
// package feast.ingestion.transform;
//
// import static feast.NormalizeFeatureRows.normalize;
// import static feast.ingestion.transform.CoalesceFeatureRows.toFeatureRow;
// import static org.hamcrest.MatcherAssert.assertThat;
// import static org.hamcrest.Matchers.equalTo;
// import static org.junit.Assert.assertEquals;
//
// import com.google.common.collect.Lists;
// import com.google.protobuf.Timestamp;
// import feast.FeastMatchers;
// import feast.NormalizeFeatureRows;
// import feast.ingestion.model.Features;
// import feast.ingestion.model.Values;
// import feast.types.FeatureProto.Feature;
// import feast.types.FeatureRowProto.FeatureRow;
// import feast_ingestion.types.CoalesceAccumProto.CoalesceAccum;
// import java.util.List;
// import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
// import org.apache.beam.sdk.testing.PAssert;
// import org.apache.beam.sdk.testing.TestPipeline;
// import org.apache.beam.sdk.testing.TestStream;
// import org.apache.beam.sdk.transforms.Count;
// import org.apache.beam.sdk.transforms.Create;
// import org.apache.beam.sdk.values.PCollection;
// import org.joda.time.Duration;
// import org.joda.time.Instant;
// import org.junit.Rule;
// import org.junit.Test;
//
// public class CoalesceFeatureRowsTest {
//
//   private static final Timestamp DEFAULT_TIMESTAMP = Timestamp.getDefaultInstance();
//   private static final FeatureRow DEFAULT_FEATURE_ROW = FeatureRow.getDefaultInstance().toBuilder()
//       .setEventTimestamp(DEFAULT_TIMESTAMP).build();
//
//   @Rule
//   public TestPipeline pipeline = TestPipeline.create();
//
//   @Test
//   public void testBatch_withDistictKeys_shouldPassThroughNonIntersectingKeys() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEventTimestamp(Timestamp.getDefaultInstance()).setEntityKey("1")
//             .build(),
//         FeatureRow.newBuilder().setEventTimestamp(Timestamp.getDefaultInstance()).setEntityKey("2")
//             .build());
//
//     PCollection<FeatureRow> input = pipeline.apply(Create.of(rows))
//         .setCoder(ProtoCoder.of(FeatureRow.class));
//
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows());
//
//     PAssert.that(output.apply(Count.globally())).containsInAnyOrder(2L);
//     PAssert.that(output).containsInAnyOrder(rows);
//
//     pipeline.run();
//   }
//
//   @Test
//   public void test_withNoFeaturesSameTimestamp_shouldReturn1() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEventTimestamp(Timestamp.getDefaultInstance()).setEntityKey("1")
//             .build(),
//         FeatureRow.newBuilder().setEventTimestamp(Timestamp.getDefaultInstance()).setEntityKey("1")
//             .build());
//     PCollection<FeatureRow> input = pipeline.apply(Create.of(rows))
//         .setCoder(ProtoCoder.of(FeatureRow.class));
//
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows());
//
//     assertThat(CoalesceFeatureRows.combineFeatureRows(rows), equalTo(rows.get(0)));
//     PAssert.that(output.apply(Count.globally())).containsInAnyOrder(1L);
//     PAssert.that(output).containsInAnyOrder(rows.get(0));
//
//     pipeline.run();
//   }
//
//   @Test
//   public void testBatch_withNoFeaturesDifferentSeconds_shouldReturnLatest() {
//     List<FeatureRow> rows1 = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(1)).build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(2)).build());
//
//     assertThat(CoalesceFeatureRows.combineFeatureRows(rows1), equalTo(rows1.get(1)));
//     assertThat(CoalesceFeatureRows.combineFeatureRows(Lists.reverse(rows1)),
//         equalTo(rows1.get(1)));
//   }
//
//
//   @Test
//   public void testBatch_withNoFeaturesDifferentNanos_shouldReturnLatest() {
//     List<FeatureRow> rows1 = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.newBuilder().setNanos(1)).build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.newBuilder().setNanos(2)).build());
//
//     assertThat(CoalesceFeatureRows.combineFeatureRows(rows1), equalTo(rows1.get(1)));
//     assertThat(CoalesceFeatureRows.combineFeatureRows(Lists.reverse(rows1)),
//         equalTo(rows1.get(1)));
//   }
//
//   @Test
//   public void testBatch_shouldMergeFeatures() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build());
//     PCollection<FeatureRow> input = pipeline.apply(Create.of(rows))
//         .setCoder(ProtoCoder.of(FeatureRow.class));
//
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows());
//
//     PAssert.that(output.apply(Count.globally())).containsInAnyOrder(1L);
//     PAssert.that(output.apply(new NormalizeFeatureRows())).containsInAnyOrder(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.getDefaultInstance())
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build()
//     );
//
//     pipeline.run();
//   }
//
//   @Test
//   public void testStream_shouldMergeFeatures() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build());
//
//     Instant start = new Instant();
//     Duration delay = Duration.standardSeconds(10);
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .advanceWatermarkTo(start)
//         .addElements(rows.get(0))
//         .addElements(rows.get(1))
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows(delay, Duration.ZERO));
//
//     PAssert.that(output.apply(Count.globally())).containsInAnyOrder(1L);
//     PAssert.that(output).containsInAnyOrder(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build()
//     );
//     pipeline.run();
//   }
//
//   @Test
//   public void testStream_shouldIncludeRowAddedOnTimerEdge() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f3").setValue(Values.ofInt32(3)))
//             .build());
//
//     Instant start = new Instant();
//     Duration delay = Duration.standardSeconds(10);
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .advanceWatermarkTo(start)
//         .addElements(rows.get(0))
//         .advanceWatermarkTo(start.plus(delay))
//         // This row will be included in the same pane because it's exactly
//         // on the same water mark as the onTimer event
//         .addElements(rows.get(1))
//         .advanceWatermarkTo(start.plus(delay).plus(delay).plus(delay))
//         .addElements(rows.get(2))
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input
//         .apply(new CoalesceFeatureRows(delay, Duration.ZERO));
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(2L));
//     PAssert.that(output.apply(new NormalizeFeatureRows())).containsInAnyOrder(
//         normalize(FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build()),
//         normalize(FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f3").setValue(Values.ofInt32(3)))
//             .build())
//     );
//     pipeline.run();
//   }
//
//   @Test
//   public void testStream_shouldMergeFeatures_emittingPanes_overlappingTimers() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build()
//     );
//
//     Instant start = new Instant();
//     Duration delay = Duration.standardSeconds(10);
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .advanceWatermarkTo(start)
//         .addElements(rows.get(0))
//         .advanceWatermarkTo(start.plus(delay.dividedBy(2)))
//         // second event before time triggers
//         .addElements(rows.get(1))
//         .advanceWatermarkTo(start.plus(delay))
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows(delay, Duration.ZERO));
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(1L));
//     PAssert.that(output.apply(new NormalizeFeatureRows())).containsInAnyOrder(
//         normalize(FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//         )
//     );
//     pipeline.run();
//   }
//
//   @Test
//   public void testStream_shouldNotSetTimerWhilePending() {
//     Instant start = new Instant();
//     Duration delay = Duration.standardSeconds(10);
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .advanceWatermarkTo(start)
//         .addElements(DEFAULT_FEATURE_ROW)
//         // this should not reset the timer as the first is still pending.
//         .advanceWatermarkTo(start.plus(delay.dividedBy(2)))
//         .addElements(DEFAULT_FEATURE_ROW)
//         // timer should trigger causing the first output row
//         .advanceWatermarkTo(start.plus(delay).plus(delay.dividedBy(2)))
//         .addElements(FeatureRow.getDefaultInstance()) // this should cause a second output row.
//         .advanceWatermarkTo(start.plus(delay).plus(delay))
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows(delay, Duration.ZERO));
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(2L));
//     PAssert.that(output).containsInAnyOrder(DEFAULT_FEATURE_ROW, DEFAULT_FEATURE_ROW);
//     pipeline.run();
//   }
//
//   @Test
//   public void testStream_shouldOnlyEmitNewFeaturesInSecondPane() {
//     Instant start = new Instant();
//     Duration delay = Duration.standardSeconds(10);
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .advanceWatermarkTo(start)
//         .addElements(
//             FeatureRow.newBuilder()
//                 .setEventTimestamp(DEFAULT_TIMESTAMP)
//                 .addFeatures(Features.of("f1", Values.ofString("a")))
//                 .build())
//         // this should should emit a row
//         .advanceWatermarkTo(start.plus(delay).plus(delay))
//         .addElements(
//             FeatureRow.newBuilder()
//                 .setEventTimestamp(DEFAULT_TIMESTAMP)
//                 .addFeatures(Features.of("f2", Values.ofString("b")))
//                 .build())
//         // this should emit a row with f2 but without f1 because it hasn't had an update
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows(delay, Duration.ZERO));
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(2L));
//     PAssert.that(output).containsInAnyOrder(
//         FeatureRow.newBuilder()
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .addFeatures(Features.of("f1", Values.ofString("a")))
//             .build(),
//         FeatureRow.newBuilder()
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .addFeatures(Features.of("f2", Values.ofString("b")))
//             .build());
//     pipeline.run();
//   }
//
//   @Test
//   public void test_combineFeatureRows_shouldCountRows() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.getDefaultInstance(),
//         FeatureRow.getDefaultInstance(),
//         FeatureRow.getDefaultInstance());
//     CoalesceAccum accum = CoalesceFeatureRows
//         .combineFeatureRowsWithSeed(CoalesceAccum.getDefaultInstance(), rows);
//     assertEquals(3, accum.getCounter());
//   }
//
//   @Test
//   public void test_combineFeatureRows_shouldOverwriteWhenLaterEventTimestampProcessedSecond() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder()
//             .addFeatures(Features.of("f1", Values.ofString("a")))
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(1))
//             .build(),
//         FeatureRow.newBuilder()
//             .addFeatures(Features.of("f1", Values.ofString("b")))
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(2))
//             .build());
//     CoalesceAccum accum = CoalesceFeatureRows
//         .combineFeatureRowsWithSeed(CoalesceAccum.getDefaultInstance(), rows);
//     assertEquals(accum.getFeaturesMap().get("f1"), Features.of("f1", Values.ofString("b")));
//   }
//
//   @Test
//   public void test_combineFeatureRows_shouldNotOverwriteWhenEarlierEventTimestampProcessedSecond() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder()
//             .addFeatures(Features.of("f1", Values.ofString("b")))
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(2))
//             .build(),
//         FeatureRow.newBuilder()
//             .addFeatures(Features.of("f1", Values.ofString("a")))
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(1))
//             .build());
//     CoalesceAccum accum = CoalesceFeatureRows
//         .combineFeatureRowsWithSeed(CoalesceAccum.getDefaultInstance(), rows);
//     assertEquals(accum.getFeaturesMap().get("f1"), Features.of("f1", Values.ofString("b")));
//   }
//
//   @Test
//   public void test_combineFeatureRows_shouldOverwriteWhenSameEventTimestampProcessedSecond() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder()
//             .addFeatures(Features.of("f1", Values.ofString("a")))
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(2))
//             .build(),
//         FeatureRow.newBuilder()
//             .addFeatures(Features.of("f1", Values.ofString("b")))
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(2))
//             .build()
//     );
//     CoalesceAccum accum = CoalesceFeatureRows
//         .combineFeatureRowsWithSeed(CoalesceAccum.getDefaultInstance(), rows);
//     assertEquals(accum.getFeaturesMap().get("f1"), Features.of("f1", Values.ofString("b")));
//   }
//
//   @Test
//   public void test_shouldPickLatestFeatures() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(
//                 Timestamp.newBuilder().setSeconds(1)) // old row with non unique feature
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(2))
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(2)))
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(1)) // old row with unique feature
//             .addFeatures(Feature.newBuilder().setId("f3").setValue(Values.ofInt32(3)))
//             .build());
//
//     CoalesceAccum accum = CoalesceFeatureRows
//         .combineFeatureRowsWithSeed(CoalesceAccum.getDefaultInstance(), rows);
//     assertEquals(3, accum.getCounter());
//     assertThat(toFeatureRow(accum, 0), equalTo(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.newBuilder().setSeconds(2))
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(2)))
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .addFeatures(Feature.newBuilder().setId("f3").setValue(Values.ofInt32(3)))
//             .build()
//     ));
//   }
//
//   @Test
//   public void testStream_withNoInput() {
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows());
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(0L));
//     pipeline.run();
//   }
//
//   @Test
//   public void testBatch_withNoInput() {
//     PCollection<FeatureRow> input = pipeline.apply(Create.empty(ProtoCoder.of(FeatureRow.class)));
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows());
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(0L));
//     pipeline.run();
//   }
//
//   @Test
//   public void testStream_withTimeout_shouldRemoveState() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build()
//     );
//
//     Instant start = new Instant();
//     Duration delay = Duration.standardSeconds(10);
//     Duration timeout = Duration.standardMinutes(30);
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .addElements(rows.get(0))
//         .advanceWatermarkTo(start.plus(timeout))
//         // first element should get fired, as the delay water mark is reached before the timeout
//         // watermark, then state should be cleared when it reaches the timeout watermark.
//         .addElements(rows.get(1))
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows(delay, timeout));
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(2L));
//     PAssert.that(output.apply(new NormalizeFeatureRows())).containsInAnyOrder(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .setEventTimestamp(Timestamp.getDefaultInstance())
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .setEventTimestamp(Timestamp.getDefaultInstance())
//             .build()
//     );
//     pipeline.run();
//   }
//
//   @Test
//   public void testStream_withDelayAfterTimeout_shouldProcessBagBeforeClear() {
//     List<FeatureRow> rows = Lists.newArrayList(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .build(),
//         FeatureRow.newBuilder().setEntityKey("1")
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build()
//     );
//
//     Instant start = new Instant();
//     Duration delay = Duration.standardMinutes(40);
//     Duration timeout = Duration.standardMinutes(30);
//     TestStream<FeatureRow> testStream = TestStream.create(ProtoCoder.of(FeatureRow.class))
//         .addElements(rows.get(0))
//         .addElements(rows.get(1))
//         // first element should get fired, as the delay water mark is reached before the timeout
//         // watermark, then state should be cleared when it reaches the timeout watermark.
//         // If it didn't process the bag before clearing it, we'd get no output events at all.
//         .advanceWatermarkToInfinity();
//
//     PCollection<FeatureRow> input = pipeline.apply(testStream);
//     PCollection<FeatureRow> output = input.apply(new CoalesceFeatureRows(delay, timeout));
//
//     PAssert.that(output).satisfies(FeastMatchers.hasCount(1L));
//     PAssert.that(output.apply(new NormalizeFeatureRows())).containsInAnyOrder(
//         FeatureRow.newBuilder().setEntityKey("1")
//             .setEventTimestamp(Timestamp.getDefaultInstance())
//             .addFeatures(Feature.newBuilder().setId("f1").setValue(Values.ofInt32(1)))
//             .addFeatures(Feature.newBuilder().setId("f2").setValue(Values.ofInt32(2)))
//             .build()
//     );
//     pipeline.run();
//   }
//
//   @Test
//   public void test_toFeatureRow_shouldBeNewMarkedFeaturesOnly() {
//     CoalesceAccum accum = CoalesceAccum.newBuilder()
//         .putFeatures("f1", Features.of("f1", Values.ofString("a")))
//         .putFeatures("f2", Features.of("f2", Values.ofString("b")))
//         .putFeatures("f3", Features.of("f3", Values.ofString("c")))
//         .putFeatures("f4", Features.of("f4", Values.ofString("d")))
//         .putFeatureMarks("f1", 1)
//         .putFeatureMarks("f2", 1)
//         .putFeatureMarks("f3", 2)
//         .putFeatureMarks("f4", 3)
//         .setCounter(3)
//         .build();
//
//     FeatureRow output = normalize(toFeatureRow(accum, 0));
//     assertThat(output,
//         equalTo(FeatureRow.newBuilder()
//             .addFeatures(Features.of("f1", Values.ofString("a")))
//             .addFeatures(Features.of("f2", Values.ofString("b")))
//             .addFeatures(Features.of("f3", Values.ofString("c")))
//             .addFeatures(Features.of("f4", Values.ofString("d")))
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .build()));
//     output = normalize(toFeatureRow(accum, 1));
//     assertThat(output,
//         equalTo(FeatureRow.newBuilder()
//             .addFeatures(Features.of("f3", Values.ofString("c")))
//             .addFeatures(Features.of("f4", Values.ofString("d")))
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .build()));
//     output = normalize(toFeatureRow(accum, 2));
//     assertThat(output,
//         equalTo(FeatureRow.newBuilder()
//             .addFeatures(Features.of("f4", Values.ofString("d")))
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .build()));
//     output = normalize(toFeatureRow(accum, 3));
//     assertThat(output,
//         equalTo(FeatureRow.newBuilder()
//             .setEventTimestamp(DEFAULT_TIMESTAMP)
//             .build()));
//   }
//
//   @Test(expected = IllegalArgumentException.class)
//   public void test_toFeatureRow_markTooHigh_shouldThrow() {
//     CoalesceAccum accum = CoalesceAccum.newBuilder()
//         .putFeatures("f1", Features.of("f1", Values.ofString("a")))
//         .putFeatures("f2", Features.of("f2", Values.ofString("b")))
//         .putFeatures("f3", Features.of("f3", Values.ofString("c")))
//         .putFeatures("f4", Features.of("f4", Values.ofString("d")))
//         .putFeatureMarks("f1", 1)
//         .putFeatureMarks("f2", 1)
//         .putFeatureMarks("f3", 2)
//         .putFeatureMarks("f4", 3)
//         .setCounter(3)
//         .build();
//     normalize(toFeatureRow(accum, 4));
//     // we throw an exception because use case should check that we have new features before trying
//     // to emit them.
//   }
// }