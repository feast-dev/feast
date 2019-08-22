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

package feast.source.kafka;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.transform.fn.FilterFeatureRowDoFn;
import feast.source.FeatureSource;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Transform for reading {@link feast.types.FeatureRowProto.FeatureRow FeatureRow} proto messages
 * from kafka one or more kafka topics.
 */
@Slf4j
@AllArgsConstructor
public class KafkaFeatureSource extends FeatureSource {

  private static final SourceType KAFKA_FEATURE_SOURCE_TYPE = SourceType.KAFKA;
  private SourceSpec sourceSpec;
  private List<String> featureIds;

  @Override
  public PCollection<FeatureRow> expand(PInput input) {
    checkArgument(sourceSpec.getType().equals(KAFKA_FEATURE_SOURCE_TYPE));
    ImportJobPipelineOptions pipelineOptions =
        input.getPipeline().getOptions().as(ImportJobPipelineOptions.class);
    String consumerGroupId = String.format("feast-import-job-%s", pipelineOptions.getJobName());

    // The following default options are used:
    // - withReadCommitted: ensures that the consumer does not read uncommitted messages so it can
    //   support end-to-end exactly-once semantics
    // - commitOffsetsInFinalize: commits the offset after finished reading the messages so if the
    //   pipeline is updated, the reader can continue from the saved offset
    // - consumer group id is derived from job id in import job spec, with prefix 'feast-import-job'
    KafkaIO.Read<byte[], FeatureRow> readPTransform =
        KafkaIO.<byte[], FeatureRow>read()
            .withBootstrapServers(sourceSpec.getOptionsOrThrow("bootstrapServers"))
            .withTopics(Arrays.asList(sourceSpec.getOptionsOrThrow("topics").split(",")))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(FeatureRowDeserializer.class)
            .withReadCommitted()
            .commitOffsetsInFinalize()
            .updateConsumerProperties(ImmutableMap.of("group.id", consumerGroupId));

    if (pipelineOptions.getSampleLimit() > 0) {
      readPTransform = readPTransform.withMaxNumRecords(pipelineOptions.getSampleLimit());
    }

    if (!pipelineOptions.isUpdate()) {
      // If import job is not an update, it is a new import job. There should be no existing Kafka
      // consumer group id and saved offset for that consumer group. In order not to miss any
      // messages that may have been published while the pipeline is being started, the default new
      // offset is set to "earliest".
      //
      // This may, however, introduce substantial lag if the subscribed topic
      // has a huge number of existing messages. If substantial lag occurs, and consumers cannot
      // catchup with the lag, Feast should probably provide a
      // configuration to set the policy for default consumer offset rather than fixing it to
      // "earliest" offset.
      log.info(
          "Pipeline will start reading from Kafka as consumer group id '{}' with 'earliest' offset "
              + "since this is a new import job (with no existing offset commited) and we do not "
              + "want to miss messages published while the pipeline is starting up and not ready.",
          consumerGroupId);
      readPTransform =
          readPTransform.updateConsumerProperties(
              ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    Pipeline pipeline = input.getPipeline();
    return pipeline
        .apply("Read with KafkaIO", readPTransform)
        .apply(
            "Transform KafkaRecord to FeatureRow",
            ParDo.of(
                new DoFn<KafkaRecord<byte[], FeatureRow>, FeatureRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext processContext) {
                    processContext.output(processContext.element().getKV().getValue());
                  }
                }))
        .apply("Filter FeatureRow", ParDo.of(new FilterFeatureRowDoFn(featureIds)));
  }
}
