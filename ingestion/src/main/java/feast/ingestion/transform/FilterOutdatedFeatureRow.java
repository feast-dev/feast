package feast.ingestion.transform;

import com.google.auto.value.AutoValue;
import feast.storage.RedisProto;
import feast.types.FeatureRowProto;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

@AutoValue
abstract public class FilterOutdatedFeatureRow
        extends PTransform<PCollection<FeatureRowProto.FeatureRow>, PCollection<FeatureRowProto.FeatureRow>> {

    public abstract Duration getStateExpiryDuration();

    public static FilterOutdatedFeatureRow.Builder newBuilder() {
        return new AutoValue_FilterOutdatedFeatureRow.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setStateExpiryDuration(Duration stateExpiryDuration);
        public abstract FilterOutdatedFeatureRow build();
    }

    private RedisProto.RedisKey getKey(FeatureRowProto.FeatureRow featureRow) {
        RedisProto.RedisKey.Builder builder = RedisProto.RedisKey.newBuilder().setFeatureSet(featureRow.getFeatureSet());
        featureRow.getFieldsList().forEach(builder::addEntities);
        return builder.build();
    }

    public static class FilterDoFn extends DoFn<KV<RedisProto.RedisKey, FeatureRowProto.FeatureRow>, FeatureRowProto.FeatureRow> {
        private static final String LAST_UPDATED = "last_updated";
        private static final String EXPIRY_TIMER = "state_expiry_timer";

        private final Duration stateExpiryDuration;

        FilterDoFn(Duration stateExpiryDuration) {
            this.stateExpiryDuration = stateExpiryDuration;
        }

        @StateId(LAST_UPDATED)
        private final StateSpec<ValueState<Long>> lastUpdatedStateSpec = StateSpecs.value(VarLongCoder.of());

        @TimerId(EXPIRY_TIMER)
        private final TimerSpec stateExpiryTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);


        @OnTimer(EXPIRY_TIMER)
        public void onExpiry(OnTimerContext context, @StateId("last_updated") ValueState<Long> lastUpdatedState) {
            lastUpdatedState.clear();
        }

        @ProcessElement
        public void processElement(ProcessContext context,
                                   @StateId("last_updated") ValueState<Long> lastUpdatedState,
                                   @TimerId("state_expiry_timer") Timer stateExpiryTimer) {
            Long lastUpdatedTimestamp = lastUpdatedState.read();
            Long currentTimestamp = context.element().getValue().getEventTimestamp().getSeconds();
            if(lastUpdatedTimestamp == null || currentTimestamp > lastUpdatedTimestamp) {
                lastUpdatedState.write(currentTimestamp);
                context.output(context.element().getValue());
                stateExpiryTimer.offset(stateExpiryDuration).setRelative();
            }
        }
    }

    @Override
    public PCollection<FeatureRowProto.FeatureRow> expand(PCollection<FeatureRowProto.FeatureRow> featureRowCollection) {
        return featureRowCollection
                .apply(
                    MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptor.of(RedisProto.RedisKey.class),
                        TypeDescriptor.of(FeatureRowProto.FeatureRow.class))
                    ).via((FeatureRowProto.FeatureRow featureRow) -> KV.of(getKey(featureRow), featureRow)))
                .apply(ParDo.of(new FilterDoFn(getStateExpiryDuration())));
    }


}
