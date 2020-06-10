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

import com.google.auto.value.AutoValue;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.IngestionJobProto;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Converts input {@link feast.proto.core.FeatureSetProto.FeatureSetSpec} into {@link
 * feast.proto.core.IngestionJobProto.FeatureSetSpecAck} message and writes it to kafka (ack-topic).
 */
@AutoValue
public abstract class WriteFeatureSetSpecAck
    extends PTransform<PCollection<KV<String, FeatureSetProto.FeatureSetSpec>>, PDone> {
  public abstract IngestionJobProto.SpecsStreamingUpdateConfig getSpecsStreamingUpdateConfig();

  public static Builder newBuilder() {
    return new AutoValue_WriteFeatureSetSpecAck.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSpecsStreamingUpdateConfig(
        IngestionJobProto.SpecsStreamingUpdateConfig config);

    public abstract WriteFeatureSetSpecAck build();
  }

  @Override
  public PDone expand(PCollection<KV<String, FeatureSetProto.FeatureSetSpec>> input) {
    return input
        .apply("FeatureSetSpecToAckMessage", ParDo.of(new BuildAckMessage()))
        .apply(
            "ToKafka",
            KafkaIO.<String, byte[]>write()
                .withBootstrapServers(
                    getSpecsStreamingUpdateConfig().getAck().getBootstrapServers())
                .withTopic(getSpecsStreamingUpdateConfig().getAck().getTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(ByteArraySerializer.class));
  }

  private static class BuildAckMessage
      extends DoFn<KV<String, FeatureSetProto.FeatureSetSpec>, KV<String, byte[]>> {
    @ProcessElement
    public void process(ProcessContext c) throws IOException {
      ByteArrayOutputStream encodedAck = new ByteArrayOutputStream();

      IngestionJobProto.FeatureSetSpecAck.newBuilder()
          .setFeatureSetReference(c.element().getKey())
          .setJobName(c.getPipelineOptions().getJobName())
          .setFeatureSetVersion(c.element().getValue().getVersion())
          .build()
          .writeTo(encodedAck);

      c.output(KV.of(c.element().getKey(), encodedAck.toByteArray()));
    }
  }
}
