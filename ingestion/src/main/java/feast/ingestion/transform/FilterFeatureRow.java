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
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.source.kafka.FeatureRowDeserializer;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


@Slf4j
public class FilterFeatureRow extends PTransform<PCollection<FeatureRow>, PCollection<FeatureRow>> {

  private String featureSetId;

  public FilterFeatureRow(FeatureSetSpec featureSetSpec) {
    this.featureSetId = String
        .format("%s:%s", featureSetSpec.getName(), featureSetSpec.getVersion());
  }

  @Override
  public PCollection<FeatureRow> expand(PCollection<FeatureRow> input) {
    return input
        .apply(
            "Filter unrelated featureRows",
            ParDo.of(
                new DoFn<FeatureRow, FeatureRow>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext c, @Element FeatureRow element) {
                    if (element.getFeatureSet().equals(featureSetId)) {
                      c.output(element);
                    }
                  }
                }));
  }
}
