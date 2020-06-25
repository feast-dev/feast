/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
 */
package feast.ingestion.transform.specs;

import static feast.ingestion.utils.SpecUtil.parseFeatureSetReference;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.IngestionJobProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;

/**
 * Source of {@link FeatureSetSpec}
 *
 * <p>Reads from Kafka topic (from the beginning) {@link FeatureSetSpec}.
 *
 * <p>Filters only relevant Specs by {@link feast.proto.core.StoreProto.Store} subscriptions and
 * {@link feast.proto.core.SourceProto.Source}.
 *
 * <p>Compacts {@link FeatureSetSpec} by reference (if it was not yet compacted in Kafka).
 */
@AutoValue
public abstract class ReadFeatureSetSpecs
    extends PTransform<PBegin, PCollection<KV<FeatureSetReference, FeatureSetSpec>>> {
  public abstract IngestionJobProto.SpecsStreamingUpdateConfig getSpecsStreamingUpdateConfig();

  public abstract SourceProto.Source getSource();

  public abstract List<StoreProto.Store> getStores();

  public static Builder newBuilder() {
    return new AutoValue_ReadFeatureSetSpecs.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSpecsStreamingUpdateConfig(
        IngestionJobProto.SpecsStreamingUpdateConfig config);

    public abstract Builder setSource(SourceProto.Source source);

    public abstract Builder setStores(List<StoreProto.Store> stores);

    public abstract ReadFeatureSetSpecs build();
  }

  @Override
  public PCollection<KV<FeatureSetReference, FeatureSetSpec>> expand(PBegin input) {
    return input
        .apply(
            KafkaIO.readBytes()
                .withBootstrapServers(
                    getSpecsStreamingUpdateConfig().getSource().getBootstrapServers())
                .withTopic(getSpecsStreamingUpdateConfig().getSource().getTopic())
                .withConsumerConfigUpdates(
                    ImmutableMap.of(
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        false)))
        .apply("ParseFeatureSetSpec", ParDo.of(new KafkaRecordToFeatureSetSpec()))
        .apply("OnlyRelevantSpecs", Filter.by(new FilterRelevantFunction(getSource(), getStores())))
        .apply(
            Window.<KV<String, FeatureSetSpec>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.ZERO))
        .apply(
            Combine.perKey(
                (SerializableFunction<Iterable<FeatureSetSpec>, FeatureSetSpec>)
                    specs -> {
                      ArrayList<FeatureSetSpec> featureSetSpecs = Lists.newArrayList(specs);
                      featureSetSpecs.sort(
                          Comparator.comparing(FeatureSetSpec::getVersion).reversed());
                      return featureSetSpecs.get(0);
                    }))
        .apply("CreateFeatureSetReferenceKey", ParDo.of(new CreateFeatureSetReference()))
        .setCoder(
            KvCoder.of(
                AvroCoder.of(FeatureSetReference.class), ProtoCoder.of(FeatureSetSpec.class)));
  }

  public static class CreateFeatureSetReference
      extends DoFn<
          KV<String, FeatureSetProto.FeatureSetSpec>,
          KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> {
    @ProcessElement
    public void process(
        ProcessContext c, @Element KV<String, FeatureSetProto.FeatureSetSpec> input) {
      Pair<String, String> reference = parseFeatureSetReference(input.getKey());
      c.output(
          KV.of(
              FeatureSetReference.of(
                  reference.getLeft(), reference.getRight(), input.getValue().getVersion()),
              input.getValue()));
    }
  }
}
