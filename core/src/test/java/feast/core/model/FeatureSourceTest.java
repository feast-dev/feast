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
package feast.core.model;

import static feast.proto.core.FeatureSourceProto.FeatureSourceSpec.SourceType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import feast.common.it.DataGenerator;
import feast.core.util.TypeConversion;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.BigQueryOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.KinesisOptions;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class FeatureSourceTest {
  @Test
  public void shouldSerializeFieldMappingAsJSON() {
    Map<String, String> expectedMap = Map.of("test", "value");

    getTestSpecs()
        .forEach(
            spec -> {
              FeatureSource source =
                  FeatureSource.fromProto(spec.toBuilder().putAllFieldMapping(expectedMap).build());
              Map<String, String> actualMap =
                  TypeConversion.convertJsonStringToMap(source.getFieldMapJSON());
              assertThat(actualMap, equalTo(actualMap));
            });
  }

  @Test
  public void shouldFromProtoBeReversableWithToProto() {
    getTestSpecs()
        .forEach(
            expectedSpec -> {
              FeatureSourceSpec actualSpec = FeatureSource.fromProto(expectedSpec).toProto();
              assertThat(actualSpec, equalTo(expectedSpec));
            });
  }

  private List<FeatureSourceSpec> getTestSpecs() {
    return List.of(
        DataGenerator.createFileFeatureSourceSpec("file:///path/to/file"),
        DataGenerator.createKafkaFeatureSourceSpec("localhost:9092", "topic", "class.path"),
        FeatureSourceSpec.newBuilder()
            .setType(BATCH_BIGQUERY)
            .setBigqueryOptions(
                BigQueryOptions.newBuilder()
                    .setProjectId("projectid")
                    .setSqlQuery("SELECT * FROM B")
                    .build())
            .build(),
        FeatureSourceSpec.newBuilder()
            .setType(STREAM_KINESIS)
            .setKinesisOptions(
                KinesisOptions.newBuilder()
                    .setRegion("ap-nowhere1")
                    .setStreamName("stream")
                    .setClassPath("class.path")
                    .build())
            .build());
  }
}
