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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import feast.ingestion.deserializer.FeatureRowDeserializer;
import feast.ingestion.deserializer.FeatureRowKeyDeserializer;
import feast.options.OptionsParser;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FeatureRowProto.FeatureRowKey;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class FeatureRowKafkaIO {

    static final String KAFKA_TYPE = "kafka";


    /**
     * Transform for reading {@link feast.types.FeatureRowProto.FeatureRow FeatureRow}
     * proto messages from kafka one or more kafka topics.
     *
     */
    public static Read read(ImportSpec importSpec) {
        return new Read(importSpec);
    }

    public static class Read extends FeatureIO.Read {

        private ImportSpec importSpec;

        private Read(ImportSpec importSpec) {
            this.importSpec = importSpec;
        }

        @Override
        public PCollection<FeatureRow> expand(PInput input) {

            checkArgument(importSpec.getType().equals(KAFKA_TYPE));

            String bootstrapServer = importSpec.getOptionsMap().get("server");

            Preconditions.checkArgument(
                    !Strings.isNullOrEmpty(bootstrapServer), "kafka bootstrap server must be set");

            String topics = importSpec.getOptionsMap().get("topics");

            Preconditions.checkArgument(
                    !Strings.isNullOrEmpty(topics), "kafka topic(s) must be set");

            List<String> topicsList = new ArrayList<>(Arrays.asList(topics.split(",")));

            KafkaIO.Read<FeatureRowKey, FeatureRow> kafkaIOReader = KafkaIO.<FeatureRowKey, FeatureRow>read()
                    .withBootstrapServers(bootstrapServer)
                    .withTopics(topicsList)
                    .withKeyDeserializer(FeatureRowKeyDeserializer.class)
                                    .withValueDeserializer(FeatureRowDeserializer.class);

            PCollection<KafkaRecord<FeatureRowKey, FeatureRow>> featureRowRecord = input.getPipeline().apply(kafkaIOReader);

            PCollection<FeatureRow> featureRow = featureRowRecord.apply(
                    ParDo.of(
                            new DoFn<KafkaRecord<FeatureRowKey, FeatureRow>, FeatureRow>() {
                                @ProcessElement
                                public void processElement(ProcessContext processContext) {
                                    KafkaRecord<FeatureRowKey, FeatureRow> record = processContext.element();
                                    processContext.output(record.getKV().getValue());
                                }
                            }));
            return featureRow;
        }
    }
}
