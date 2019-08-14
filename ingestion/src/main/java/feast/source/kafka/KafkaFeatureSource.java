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

import com.google.auto.service.AutoService;
import feast.ingestion.options.ImportJobPipelineOptions;
import feast.ingestion.transform.fn.FilterFeatureRowDoFn;
import feast.options.Options;
import feast.source.FeatureSource;
import feast.source.FeatureSourceFactory;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import feast.types.FeatureRowProto.FeatureRow;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.validation.constraints.NotEmpty;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Transform for reading {@link feast.types.FeatureRowProto.FeatureRow FeatureRow} proto messages
 * from kafka one or more kafka topics.
 */
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

    // KafkaReadOptions options =
    //     OptionsParser.parse(sourceSpec.getOptionsMap(), KafkaReadOptions.class);
    //
    // List<String> topicsList = new ArrayList<>(Arrays.asList(options.topics.split(",")));

    KafkaIO.Read<byte[], FeatureRow> read =
        KafkaIO.<byte[], FeatureRow>read()
            .withBootstrapServers(sourceSpec.getOptionsOrThrow("bootstrapServers"))
            .withTopics(Arrays.asList(sourceSpec.getOptionsOrThrow("topics").split(",")))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(FeatureRowDeserializer.class);

    if (pipelineOptions.getSampleLimit() > 0) {
      read = read.withMaxNumRecords(pipelineOptions.getSampleLimit());
    }

    PCollection<KafkaRecord<byte[], FeatureRow>> featureRowRecord = input.getPipeline().apply(read);

    PCollection<FeatureRow> featureRow =
        featureRowRecord.apply(
            ParDo.of(
                new DoFn<KafkaRecord<byte[], FeatureRow>, FeatureRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext processContext) {
                    KafkaRecord<byte[], FeatureRow> record = processContext.element();
                    processContext.output(record.getKV().getValue());
                  }
                }));

    return featureRow.apply(ParDo.of(new FilterFeatureRowDoFn(featureIds)));
  }

  public static class KafkaReadOptions implements Options {

    @NotEmpty public String server;
    @NotEmpty public String topics;
    public boolean discardUnknownFeatures = false;
  }

  @AutoService(FeatureSourceFactory.class)
  public static class Factory implements FeatureSourceFactory {

    @Override
    public SourceType getType() {
      return KAFKA_FEATURE_SOURCE_TYPE;
    }

    @Override
    public FeatureSource create(SourceSpec sourceSpec, List<String> featureIds) {
      checkArgument(sourceSpec.getType().equals(getType()));
      return new KafkaFeatureSource(sourceSpec, featureIds);
    }
  }
}
