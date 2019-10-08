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

package feast.ingestion.transform;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Arrays;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class ReadFeatureRow extends PTransform<PInput, PCollection<FeatureRow>> {
  private FeatureSetSpec featureSetSpec;

  public ReadFeatureRow(FeatureSetSpec featureSetSpec) {
    this.featureSetSpec = featureSetSpec;
  }

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    SourceType sourceType = featureSetSpec.getSource().getType();

    if (!sourceType.equals(SourceType.KAFKA)) {
      throw new IllegalArgumentException(
          "Only SourceType.KAFKA is supported for Source in Feast import job.");
    }

    String kafkaConsumerGroupId =
        String.format("feast-import-job-%s", input.getPipeline().getOptions().getJobName());

    KafkaSourceConfig kafkaSourceConfig = featureSetSpec.getSource().getKafkaSourceConfig();
    return input
        .getPipeline()
        .apply(
            "Read from Kafka",
            KafkaIO.<byte[], byte[]>read()
                .withBootstrapServers(kafkaSourceConfig.getBootstrapServers())
                .withTopics(Arrays.asList(kafkaSourceConfig.getTopic()))
                .withKeyDeserializer(ByteArrayDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withReadCommitted()
                .commitOffsetsInFinalize()
                .updateConsumerProperties(ImmutableMap.of("group.id", kafkaConsumerGroupId)))
        .apply(
            "Convert KafkaRecord into FeatureRow",
            ParDo.of(
                new DoFn<KafkaRecord<byte[], byte[]>, FeatureRow>() {
                  @ProcessElement
                  public void processElemennt(ProcessContext context) {
                    try {
                      FeatureRow featureRow =
                          FeatureRow.parseFrom(context.element().getKV().getValue());
                      context.output(featureRow);
                    } catch (InvalidProtocolBufferException e) {
                      e.printStackTrace();
                    }
                  }
                }));
  }
}
