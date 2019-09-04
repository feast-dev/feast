// /*
//  * Copyright 2019 The Feast Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
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
// import com.google.common.base.Preconditions;
// import com.google.protobuf.util.Timestamps;
// import feast.types.FeatureProto.Feature;
// import feast.types.FeatureRowProto.FeatureRow;
// import feast_ingestion.types.CoalesceAccumProto.CoalesceAccum;
// import feast_ingestion.types.CoalesceKeyProto.CoalesceKey;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collectors;
// import lombok.AllArgsConstructor;
// import lombok.extern.slf4j.Slf4j;
// import org.apache.beam.sdk.coders.KvCoder;
// import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
// import org.apache.beam.sdk.state.BagState;
// import org.apache.beam.sdk.state.StateSpec;
// import org.apache.beam.sdk.state.StateSpecs;
// import org.apache.beam.sdk.state.TimeDomain;
// import org.apache.beam.sdk.state.Timer;
// import org.apache.beam.sdk.state.TimerSpec;
// import org.apache.beam.sdk.state.TimerSpecs;
// import org.apache.beam.sdk.state.ValueState;
// import org.apache.beam.sdk.transforms.Combine;
// import org.apache.beam.sdk.transforms.DoFn;
// import org.apache.beam.sdk.transforms.PTransform;
// import org.apache.beam.sdk.transforms.ParDo;
// import org.apache.beam.sdk.transforms.SerializableFunction;
// import org.apache.beam.sdk.transforms.Values;
// import org.apache.beam.sdk.transforms.WithKeys;
// import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
// import org.apache.beam.sdk.transforms.windowing.Window;
// import org.apache.beam.sdk.values.KV;
// import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.values.PCollection.IsBounded;
// import org.apache.beam.sdk.values.TypeDescriptor;
// import org.joda.time.Duration;
// import org.joda.time.Instant;
//
// /**
//  * Takes FeatureRow, and merges them if they have the same FeatureRowKey, so that the latest values
//  * will be emitted. It emits only once for batch.
//  *
//  * <p>For streaming we emits after a delay of 10 seconds (event time) by default we keep the
//  * previous state around for merging with future events. These timeout after 30 minutes by default.
//  */
// public class CoalesceFeatureRows
//     extends PTransform<PCollection<FeatureRow>, PCollection<FeatureRow>> {
//
//   private static final SerializableFunction<FeatureRow, CoalesceKey> KEY_FUNCTION =
//       (row) ->
//           CoalesceKey.newBuilder()
//               .setEntityName(row.getEntityName())
//               .setEntityKey(row.getEntityKey())
//               .build();
//
//   private static final Duration DEFAULT_DELAY = Duration.standardSeconds(10);
//   private static final Duration DEFAULT_TIMEOUT = Duration.ZERO;
//
//   private Duration delay;
//   private Duration timeout;
//
//   CoalesceFeatureRows() {
//     this(0, 0);
//   }
//
//   public CoalesceFeatureRows(long delaySeconds, long timeoutSeconds) {
//     this(Duration.standardSeconds(delaySeconds), Duration.standardSeconds(timeoutSeconds));
//   }
//
//   public CoalesceFeatureRows(Duration delay, Duration timeout) {
//     this.delay = (delay.isEqual(Duration.ZERO)) ? DEFAULT_DELAY : delay;
//     this.timeout = (timeout.isEqual(Duration.ZERO)) ? DEFAULT_TIMEOUT : timeout;
//   }
//
//   /** Return a FeatureRow of the new features accumulated since the given timestamp */
//   public static FeatureRow toFeatureRow(CoalesceAccum accum, long counter) {
//     Preconditions.checkArgument(
//         counter <= accum.getCounter(),
//         "Accumulator has no features at or newer than the provided counter");
//     FeatureRow.Builder builder =
//         FeatureRow.newBuilder()
//             .setEntityName(accum.getEntityName())
//             .setEntityKey(accum.getEntityKey());
//     // This will be the latest timestamp
//     if (accum.hasEventTimestamp()) {
//       builder.setEventTimestamp(accum.getEventTimestamp());
//     }
//
//     Map<String, Feature> features = accum.getFeaturesMap();
//     if (counter <= 0) {
//       builder.addAllFeatures(features.values());
//     } else {
//       List<Feature> featureList =
//           accum.getFeatureMarksMap().entrySet().stream()
//               .filter((e) -> e.getValue() > counter)
//               .map((e) -> features.get(e.getKey()))
//               .collect(Collectors.toList());
//       builder.addAllFeatures(featureList);
//     }
//     return builder.build();
//   }
//
//   public static FeatureRow combineFeatureRows(Iterable<FeatureRow> rows) {
//     return toFeatureRow(combineFeatureRowsWithSeed(CoalesceAccum.getDefaultInstance(), rows), 0);
//   }
//
//   public static CoalesceAccum combineFeatureRowsWithSeed(
//       CoalesceAccum seed, Iterable<FeatureRow> rows) {
//     CoalesceAccum.Builder accum = seed.toBuilder();
//     Map<String, Feature> features = new HashMap<>();
//     Map<String, Long> featureMarks = new HashMap<>();
//     long rowCount = seed.getCounter();
//     for (FeatureRow row : rows) {
//       rowCount += 1;
//       if (Timestamps.compare(accum.getEventTimestamp(), row.getEventTimestamp()) <= 0) {
//         // row has later timestamp than accum.
//         for (Feature feature : row.getFeaturesList()) {
//           features.put(feature.getId(), feature);
//           // These marks are used to determine which features are new when we convert an accum
//           // back into a FeatureRow.
//           featureMarks.put(feature.getId(), rowCount);
//         }
//         accum.setEntityName(row.getEntityName());
//         accum.setEntityKey(row.getEntityKey());
//         if (row.hasEventTimestamp()) {
//           accum.setEventTimestamp(row.getEventTimestamp());
//         }
//       } else {
//         for (Feature feature : row.getFeaturesList()) {
//           String featureId = feature.getId();
//           // only insert an older feature if there was no newer one.
//           if (!features.containsKey(featureId)) {
//             features.put(featureId, feature);
//             featureMarks.put(feature.getId(), rowCount);
//           }
//         }
//       }
//     }
//     if (rowCount == seed.getCounter()) {
//       return seed;
//     } else {
//       return accum
//           .setCounter(rowCount)
//           .putAllFeatures(features)
//           .putAllFeatureMarks(featureMarks)
//           .build();
//     }
//   }
//
//   @Override
//   public PCollection<FeatureRow> expand(PCollection<FeatureRow> input) {
//     PCollection<KV<CoalesceKey, FeatureRow>> kvs =
//         input
//             .apply(WithKeys.of(KEY_FUNCTION).withKeyType(TypeDescriptor.of(CoalesceKey.class)))
//             .setCoder(
//                 KvCoder.of(ProtoCoder.of(CoalesceKey.class), ProtoCoder.of(FeatureRow.class)));
//
//     if (kvs.isBounded().equals(IsBounded.UNBOUNDED)) {
//       return kvs.apply(
//               "Configure window",
//               Window.<KV<CoalesceKey, FeatureRow>>configure()
//                   .withAllowedLateness(Duration.ZERO)
//                   .discardingFiredPanes()
//                   .triggering(AfterProcessingTime.pastFirstElementInPane()))
//           .apply(ParDo.of(new CombineStateDoFn(delay, timeout)))
//           .apply(Values.create());
//     } else {
//       return kvs.apply(Combine.perKey(CoalesceFeatureRows::combineFeatureRows))
//           .apply(Values.create());
//     }
//   }
//
//   @Slf4j
//   @AllArgsConstructor
//   public static class CombineStateDoFn
//       extends DoFn<KV<CoalesceKey, FeatureRow>, KV<CoalesceKey, FeatureRow>> {
//
//     @StateId("lastKnownAccumValue")
//     private final StateSpec<ValueState<CoalesceAccum>> lastKnownAccumValueSpecs =
//         StateSpecs.value(ProtoCoder.of(CoalesceAccum.class));
//
//     @StateId("newElementsBag")
//     private final StateSpec<BagState<FeatureRow>> newElementsBag =
//         StateSpecs.bag(ProtoCoder.of(FeatureRow.class));
//
//     @StateId("lastTimerTimestamp")
//     private final StateSpec<ValueState<Instant>> lastTimerTimestamp = StateSpecs.value();
//
//     @TimerId("bufferTimer")
//     private final TimerSpec bufferTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);
//
//     @TimerId("timeoutTimer")
//     private final TimerSpec timeoutTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);
//
//     private Duration delay;
//     private Duration timeout;
//
//     @ProcessElement
//     public void processElement(
//         ProcessContext context,
//         @StateId("newElementsBag") BagState<FeatureRow> newElementsBag,
//         @TimerId("bufferTimer") Timer bufferTimer,
//         @TimerId("timeoutTimer") Timer timeoutTimer,
//         @StateId("lastTimerTimestamp") ValueState<Instant> lastTimerTimestampValue) {
//       newElementsBag.add(context.element().getValue());
//       log.debug("Adding FeatureRow to state bag {}", context.element());
//
//       Instant lastTimerTimestamp = lastTimerTimestampValue.read();
//       Instant contextTimestamp = context.timestamp();
//       if (lastTimerTimestamp == null && timeout.isLongerThan(Duration.ZERO)) {
//         // We never timeout the state if a timeout is not set.
//         timeoutTimer.offset(timeout).setRelative();
//       }
//       if (lastTimerTimestamp == null
//           || lastTimerTimestamp.isBefore(contextTimestamp)
//           || lastTimerTimestamp.equals(contextTimestamp)) {
//         lastTimerTimestamp = context.timestamp().plus(delay);
//         log.debug("Setting timer for key {} to {}", context.element().getKey(), lastTimerTimestamp);
//         lastTimerTimestampValue.write(lastTimerTimestamp);
//         bufferTimer.offset(delay).setRelative();
//       }
//     }
//
//     @OnTimer("bufferTimer")
//     public void bufferOnTimer(
//         OnTimerContext context,
//         OutputReceiver<KV<CoalesceKey, FeatureRow>> out,
//         @StateId("newElementsBag") BagState<FeatureRow> newElementsBag,
//         @StateId("lastKnownAccumValue") ValueState<CoalesceAccum> lastKnownAccumValue) {
//       log.debug("bufferOnTimer triggered {}", context.timestamp());
//       flush(out, newElementsBag, lastKnownAccumValue);
//     }
//
//     @OnTimer("timeoutTimer")
//     public void timeoutOnTimer(
//         OnTimerContext context,
//         OutputReceiver<KV<CoalesceKey, FeatureRow>> out,
//         @StateId("newElementsBag") BagState<FeatureRow> newElementsBag,
//         @StateId("lastKnownAccumValue") ValueState<CoalesceAccum> lastKnownAccumValue) {
//       log.debug("timeoutOnTimer triggered {}", context.timestamp());
//       flush(out, newElementsBag, lastKnownAccumValue);
//       newElementsBag.clear();
//       lastKnownAccumValue.clear();
//     }
//
//     public void flush(
//         OutputReceiver<KV<CoalesceKey, FeatureRow>> out,
//         @StateId("newElementsBag") BagState<FeatureRow> newElementsBag,
//         @StateId("lastKnownAccumValue") ValueState<CoalesceAccum> lastKnownAccumValue) {
//       log.debug("Flush triggered");
//       Iterable<FeatureRow> rows = newElementsBag.read();
//       if (!rows.iterator().hasNext()) {
//         log.debug("Flush with no new elements");
//         return;
//       }
//       CoalesceAccum lastKnownAccum = lastKnownAccumValue.read();
//       if (lastKnownAccum == null) {
//         lastKnownAccum = CoalesceAccum.getDefaultInstance();
//       }
//       // Check if we have more than one value in our list.
//       CoalesceAccum accum = combineFeatureRowsWithSeed(lastKnownAccum, rows);
//       FeatureRow row = toFeatureRow(accum, lastKnownAccum.getCounter());
//       log.debug("Timer fired and added FeatureRow to output {}", row);
//       // Clear the elements now that they have been processed
//       newElementsBag.clear();
//       lastKnownAccumValue.write(accum);
//
//       // Output the value stored in the the processed que which matches this timers time
//       out.output(KV.of(KEY_FUNCTION.apply(row), row));
//     }
//   }
// }
