package feast.ingestion.transform;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.protobuf.Timestamp;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class CoalesceFeatureRows extends
    PTransform<PCollection<FeatureRow>, PCollection<FeatureRow>> {

  private static final Comparator<Timestamp> TIMESTAMP_COMPARATOR = Comparator
      .comparing(Timestamp::getSeconds)
      .thenComparing(Timestamp::getNanos);
  private static final SerializableFunction<FeatureRow, String> KEY_FUNCTION = (row) ->
      row.getEntityName() + "=" + row
          .getEntityKey();
  private Duration delay = Duration.standardSeconds(10);

  public static FeatureRow combineFeatureRows(Iterable<FeatureRow> rows) {
    FeatureRow latestRow = null;
    Map<String, Feature> features = new HashMap<>();
    int rowCount = 0;
    for (FeatureRow row : rows) {
      rowCount += 1;
      if (latestRow == null) {
        latestRow = row;
      } else {
        if (TIMESTAMP_COMPARATOR.compare(latestRow.getEventTimestamp(), row.getEventTimestamp())
            < 0) {
          // row has later timestamp than agg.
          for (Feature feature : row.getFeaturesList()) {
            features.put(feature.getId(), feature);
          }
          latestRow = row;
        } else {
          for (Feature feature : row.getFeaturesList()) {
            String featureId = feature.getId();
            // only insert an older feature if there was no newer one.
            if (!features.containsKey(featureId)) {
              features.put(featureId, feature);
            }
          }
        }
      }
    }

    Preconditions.checkNotNull(latestRow);
    if (rowCount == 1) {
      return latestRow;
    } else {
      for (Feature feature : latestRow.getFeaturesList()) {
        features.put(feature.getId(), feature);
      }
      return latestRow.toBuilder().clearFeatures().addAllFeatures(features.values()).build();
    }
  }

  CoalesceFeatureRows withDelay(Duration delay) {
    this.delay = delay;
    return this;
  }

  @Override
  public PCollection<FeatureRow> expand(PCollection<FeatureRow> input) {

    PCollection<KV<String, FeatureRow>> kvs = input
        .apply(WithKeys.of(KEY_FUNCTION).withKeyType(TypeDescriptors.strings()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(FeatureRow.class)));

    if (kvs.isBounded().equals(IsBounded.UNBOUNDED)) {
      return kvs.apply("Configure window", Window.<KV<String, FeatureRow>>configure()
          .withAllowedLateness(Duration.ZERO)
          .discardingFiredPanes()
          .triggering(AfterProcessingTime.pastFirstElementInPane()))
          .apply(ParDo.of(new CombineStateDoFn(delay)))
          .apply(Values.create());
    } else {
      return kvs.apply(Combine.perKey(CoalesceFeatureRows::combineFeatureRows))
          .apply(Values.create());
    }
  }

  @Slf4j
  @AllArgsConstructor
  public static class CombineStateDoFn extends
      DoFn<KV<String, FeatureRow>, KV<String, FeatureRow>> {

    @StateId("lastKnownValue")
    private final StateSpec<ValueState<FeatureRow>> lastKnownValue =
        StateSpecs.value(ProtoCoder.of(FeatureRow.class));
    @StateId("newElementsBag")
    private final StateSpec<BagState<FeatureRow>> newElementsBag =
        StateSpecs.bag(ProtoCoder.of(FeatureRow.class));
    @StateId("lastTimerTimestamp")
    private final StateSpec<ValueState<Instant>> lastTimerTimestamp = StateSpecs.value();
    @TimerId("bufferTimer")
    private final TimerSpec bufferTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);
    private Duration delay;

    @ProcessElement
    public void processElement(ProcessContext context,
        @StateId("newElementsBag") BagState<FeatureRow> newElementsBag,
        @TimerId("bufferTimer") Timer bufferTimer,
        @StateId("lastTimerTimestamp") ValueState<Instant> lastTimerTimestampValue) {
      newElementsBag.add(context.element().getValue());
      log.debug("Adding FeatureRow to state bag {}", context.element());

      Instant lastTimerTimestamp = lastTimerTimestampValue.read();
      Instant contextTimestamp = context.timestamp();
      if (lastTimerTimestamp == null || lastTimerTimestamp.isBefore(contextTimestamp) || lastTimerTimestamp.equals(contextTimestamp)) {
        lastTimerTimestamp = context.timestamp().plus(delay);
        log.debug("Setting timer for key {} to {}", context.element().getKey(), lastTimerTimestamp);
        lastTimerTimestampValue.write(lastTimerTimestamp);
        bufferTimer.offset(delay).setRelative();
      }
    }

    @OnTimer("bufferTimer")
    public void bufferOnTimer(
        OnTimerContext context, OutputReceiver<KV<String, FeatureRow>> out,
        @StateId("newElementsBag") BagState<FeatureRow> newElementsBag,
        @StateId("lastKnownValue") ValueState<FeatureRow> lastKnownValue) {
      log.debug("onTimer triggered {}", context.timestamp());
      Iterable<FeatureRow> rows = newElementsBag.read();
      if (!rows.iterator().hasNext()) {
        log.debug("Flush with no new elements");
        return;
      }
      FeatureRow lastKnown = lastKnownValue.read();
      if (lastKnown != null) {
        rows = Iterables.concat(Collections.singleton(lastKnown), newElementsBag.read());
      }
      // Check if we have more than one value in our list.
      FeatureRow row = combineFeatureRows(rows);
      log.debug("Timer fired and added FeatureRow to output {}", row);
      // Clear the elements now that they have been processed
      newElementsBag.clear();
      lastKnownValue.write(row);

      // Output the value stored in the the processed que which matches this timers time
      out.output(KV.of(KEY_FUNCTION.apply(row), row));
    }
  }
}
