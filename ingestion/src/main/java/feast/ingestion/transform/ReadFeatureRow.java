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

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.ingestion.options.ImportOptions;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.TupleTag;

public class ReadFeatureRow extends PTransform<PInput, PCollectionTuple> {
  private final FeatureSetSpec featureSetSpec;
  private final ImportOptions options;
  private final String consumerGroupId;
  private final KafkaSourceConfig sourceConfig;

  public ReadFeatureRow(FeatureSetSpec featureSetSpec, ImportOptions options) {
    this.featureSetSpec = featureSetSpec;
    this.options = options;
    this.consumerGroupId = String.format("feast-import-job-%s", options.getJobName());
    this.sourceConfig = featureSetSpec.getSource().getKafkaSourceConfig();
  }

  @Override
  public PCollectionTuple expand(PInput input) {
    if (!featureSetSpec.getSource().getType().equals(SourceType.KAFKA)) {
      throw new IllegalArgumentException(
          "Only SourceType.KAFKA is supported for Source in Feast import job.");
    }

    PCollection<KafkaRecord<byte[], byte[]>> out = input.getPipeline().apply(
        KafkaIO.readBytes().withBootstrapServers(sourceConfig.getBootstrapServers())
            .withTopic(sourceConfig.getTopic()));

    // input
    //     .getPipeline()
    //     .apply(
    //         "ReadFromKafka",
    //         KafkaIO.<byte[], byte[]>read()
    //             .withBootstrapServers(sourceConfig.getBootstrapServers())
    //             .withTopic(sourceConfig.getTopic())
    //             .withKeyDeserializer(ByteArrayDeserializer.class)
    //             .withValueDeserializer(ByteArrayDeserializer.class)
    //             //.withConsumerConfigUpdates(ImmutableMap.of("group.id", consumerGroupId))
    //             //.withReadCommitted()
    //             //.commitOffsetsInFinalize()
    //     )
    //     .apply(ParDo.of(new DoFn<KafkaRecord<byte[],byte[]>, KafkaRecord<byte[],byte[]>>() {
    //       @ProcessElement
    //       public void processElement(ProcessContext context) {
    //         KafkaRecord<byte[], byte[]> element = context.element();
    //         byte[] value = element.getKV().getValue();
    //         context.output(element);
    //       }
    //     }));
        //.apply("KafkaMessageToFeatureRow", new KafkaMessageToFeatureRow(options));

    return PCollectionTuple.of("TODO", out);

    // .apply(
    //     "Convert KafkaRecord into FeatureRow",
    //     ParDo.of(
    //         new DoFn<KafkaRecord<byte[], byte[]>, FeatureRow>() {
    //           @ProcessElement
    //           public void processElemennt(ProcessContext context) {
    //             try {
    //               FeatureRow featureRow =
    //                   FeatureRow.parseFrom(context.element().getKV().getValue());
    //               context.output(featureRow);
    //             } catch (InvalidProtocolBufferException e) {
    //               e.printStackTrace();
    //             }
    //           }
    //         }));
  }

  static class KafkaMessageToFeatureRow
      extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollectionTuple> {

    private final ImportOptions options;

    KafkaMessageToFeatureRow(ImportOptions options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
      TupleTag<FeatureRow> featureRowTupleTag = new TupleTag<>();

      return input.apply(
          "TODO",
          ParDo.of(
                  new DoFn<KafkaRecord<byte[], byte[]>, FeatureRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      // byte[] value = context.element().getKV().getValue();
                      // try {
                      //   FeatureRow featureRow = FeatureRow.parseFrom(value);
                      //   context.output(featureRow);
                      // } catch (InvalidProtocolBufferException e) {
                      //   e.printStackTrace();
                      // }
                    }
                  })
              .withOutputTags(featureRowTupleTag, null));
    }
  }
}
