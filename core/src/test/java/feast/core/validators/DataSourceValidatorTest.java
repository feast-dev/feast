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
package feast.core.validators;

import static feast.proto.core.DataSourceProto.DataSource.SourceType.*;

import feast.common.it.DataGenerator;
import feast.proto.core.DataSourceProto;
import feast.proto.core.DataSourceProto.DataSource.BigQueryOptions;
import feast.proto.core.DataSourceProto.DataSource.KafkaOptions;
import feast.proto.core.DataSourceProto.DataSource.KinesisOptions;
import feast.proto.core.DataSourceProto.DataSource.SourceType;
import java.util.Map;
import org.junit.Test;

public class DataSourceValidatorTest {

  @Test(expected = UnsupportedOperationException.class)
  public void shouldErrorIfSourceTypeUnsupported() {
    DataSourceProto.DataSource badSpec =
        getTestSpecsMap().get(BATCH_FILE).toBuilder().setType(SourceType.INVALID).build();
    DataSourceValidator.validate(badSpec);
  }

  @Test
  public void shouldPassValidSpecs() {
    getTestSpecsMap().values().forEach(DataSourceValidator::validate);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfBadBigQueryTableRef() {
    DataSourceProto.DataSource badSpec =
        getTestSpecsMap()
            .get(BATCH_BIGQUERY)
            .toBuilder()
            .setBigqueryOptions(BigQueryOptions.newBuilder().setTableRef("bad:/ref").build())
            .build();
    DataSourceValidator.validate(badSpec);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfBadClassPath() {
    DataSourceProto.DataSource badSpec =
        getTestSpecsMap()
            .get(STREAM_KAFKA)
            .toBuilder()
            .setKafkaOptions(
                KafkaOptions.newBuilder()
                    .setMessageFormat(DataGenerator.createProtoFormat(".bad^path"))
                    .build())
            .build();
    DataSourceValidator.validate(badSpec);

    badSpec =
        getTestSpecsMap()
            .get(STREAM_KINESIS)
            .toBuilder()
            .setKinesisOptions(
                KinesisOptions.newBuilder()
                    .setRecordFormat(DataGenerator.createProtoFormat(".bad^path"))
                    .build())
            .build();
    DataSourceValidator.validate(badSpec);
  }

  private Map<SourceType, DataSourceProto.DataSource> getTestSpecsMap() {
    return Map.of(
        BATCH_FILE, DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""),
        BATCH_BIGQUERY,
            DataGenerator.createBigQueryDataSourceSpec("project:dataset.table", "ts_col", "dt_col"),
        STREAM_KINESIS,
            DataGenerator.createKinesisDataSourceSpec(
                "ap-nowhere1", "stream", "class.path", "ts_col"),
        STREAM_KAFKA,
            DataGenerator.createKafkaDataSourceSpec(
                "localhost:9092", "topic", "class.path", "ts_col"));
  }
}
