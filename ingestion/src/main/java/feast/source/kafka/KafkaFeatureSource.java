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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Instant;

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

    // The following default options are used:
    // - withReadCommitted: ensures that the consumer does not read uncommitted message so it can
    //   support end-to-end exactly-once semantics
    // - commitOffsetsInFinalize: commit the offset after finished reading so if the pipeline is
    //   updated, the reader can continue from the saved offset
    // - consumer group is derived from job id in import job spec
    KafkaIO.Read<byte[], FeatureRow> readPTransform =
        KafkaIO.<byte[], FeatureRow>read()
            .withBootstrapServers(sourceSpec.getOptionsOrThrow("bootstrapServers"))
            .withTopics(Arrays.asList(sourceSpec.getOptionsOrThrow("topics").split(",")))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(FeatureRowDeserializer.class)
            .withReadCommitted()
            .commitOffsetsInFinalize()
            .updateConsumerProperties(
                ImmutableMap.of(
                    "group.id",
                    String.format("feast-import-job-%s", pipelineOptions.getJobName())));

    if (pipelineOptions.getSampleLimit() > 0) {
      readPTransform = readPTransform.withMaxNumRecords(pipelineOptions.getSampleLimit());
    }

    if (!pipelineOptions.isUpdate()) {
      // If import job is not an update, there will be no existing consumer group id
      // So we cannot depend on the consumer group commit offset to determine when to start reading
      // Kafka messages. A reasonable approach is to start reading from the timestamp when
      // this transform step in the pipeline is created. We do not use the timestamp when the
      // pipeline starts running because it takes some time to provision workers and
      // by the the time the pipeline starts running, some messages may have already been published
      // and we may miss the messages.
      Instant kafkaStartReadTime = new Instant();
      readPTransform = readPTransform.withStartReadTime(kafkaStartReadTime);
      log.info(
          "Pipeline will start reading from Kafka with offset timestamp {}", kafkaStartReadTime);
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
