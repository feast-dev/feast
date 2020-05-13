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
package feast.core.job.option;

import static org.junit.Assert.*;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto;
import feast.core.SourceProto;
import feast.types.ValueProto;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class FeatureSetJsonByteConverterTest {

  private FeatureSetProto.FeatureSet newFeatureSet(Integer numberOfFeatures) {
    List<FeatureSetProto.FeatureSpec> features =
        IntStream.range(1, numberOfFeatures + 1)
            .mapToObj(
                i ->
                    FeatureSetProto.FeatureSpec.newBuilder()
                        .setValueType(ValueProto.ValueType.Enum.FLOAT)
                        .setName("feature".concat(Integer.toString(i)))
                        .build())
            .collect(Collectors.toList());

    return FeatureSetProto.FeatureSet.newBuilder()
        .setSpec(
            FeatureSetProto.FeatureSetSpec.newBuilder()
                .setSource(
                    SourceProto.Source.newBuilder()
                        .setType(SourceProto.SourceType.KAFKA)
                        .setKafkaSourceConfig(
                            SourceProto.KafkaSourceConfig.newBuilder()
                                .setBootstrapServers("somebrokers:9092")
                                .setTopic("sometopic")))
                .addAllFeatures(features)
                .addEntities(
                    FeatureSetProto.EntitySpec.newBuilder()
                        .setName("entity")
                        .setValueType(ValueProto.ValueType.Enum.STRING)))
        .build();
  }

  @Test
  public void shouldConvertFeatureSetsAsJsonStringBytes() throws InvalidProtocolBufferException {
    int nrOfFeatureSet = 1;
    int nrOfFeatures = 1;
    List<FeatureSetProto.FeatureSet> featureSets =
        IntStream.range(1, nrOfFeatureSet + 1)
            .mapToObj(i -> newFeatureSet(nrOfFeatures))
            .collect(Collectors.toList());

    String expectedOutputString =
        "{\"entities\":[{\"name\":\"entity\",\"valueType\":2}],"
            + "\"features\":[{\"name\":\"feature1\",\"valueType\":6}],"
            + "\"source\":{"
            + "\"type\":1,"
            + "\"kafkaSourceConfig\":{"
            + "\"bootstrapServers\":\"somebrokers:9092\","
            + "\"topic\":\"sometopic\"}}}";
    FeatureSetJsonByteConverter byteConverter = new FeatureSetJsonByteConverter();
    assertEquals(expectedOutputString, new String(byteConverter.toByte(featureSets)));
  }
}
