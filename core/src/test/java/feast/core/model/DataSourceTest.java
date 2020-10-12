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

import static feast.proto.core.DataSourceProto.DataSource.SourceType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import feast.common.it.DataGenerator;
import feast.proto.core.DataSourceProto;
import feast.proto.core.DataSourceProto.DataFormat;
import feast.proto.core.DataSourceProto.DataFormat.ProtoFormat;
import feast.proto.core.DataSourceProto.DataSource.BigQueryOptions;
import feast.proto.core.DataSourceProto.DataSource.KinesisOptions;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DataSourceTest {
  @Test
  public void shouldSerializeFieldMappingAsJSON() {
    Map<String, String> expectedMap = Map.of("test", "value");

    getTestSpecs()
        .forEach(
            spec -> {
              DataSource source =
                  DataSource.fromProto(spec.toBuilder().putAllFieldMapping(expectedMap).build());
              Map<String, String> actualMap = source.getFieldsMap();
              assertThat(actualMap, equalTo(actualMap));
            });
  }

  @Test
  public void shouldFromProtoBeReversableWithToProto() {
    getTestSpecs()
        .forEach(
            expectedSpec -> {
              DataSourceProto.DataSource actualSpec = DataSource.fromProto(expectedSpec).toProto();
              assertThat(actualSpec, equalTo(expectedSpec));
            });
  }

  private List<DataSourceProto.DataSource> getTestSpecs() {
    return List.of(
        DataGenerator.createFileDataSourceSpec("file:///path/to/file", "parquet", "ts_col", ""),
        DataGenerator.createKafkaDataSourceSpec("localhost:9092", "topic", "class.path", "ts_col"),
        DataSourceProto.DataSource.newBuilder()
            .setType(BATCH_BIGQUERY)
            .setBigqueryOptions(
                BigQueryOptions.newBuilder().setTableRef("project:dataset.table").build())
            .build(),
        DataSourceProto.DataSource.newBuilder()
            .setType(STREAM_KINESIS)
            .setKinesisOptions(
                KinesisOptions.newBuilder()
                    .setRegion("ap-nowhere1")
                    .setStreamName("stream")
                    .setRecordFormat(
                        DataFormat.newBuilder()
                            .setProtoFormat(
                                ProtoFormat.newBuilder().setClassPath("class.path").build())
                            .build())
                    .build())
            .build());
  }
}
