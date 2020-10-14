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

import static feast.proto.core.DataSourceProto.DataFormat.FormatCase.*;
import static feast.proto.core.DataSourceProto.DataSource.SourceType.*;
import static org.junit.Assert.assertTrue;

import feast.common.it.DataGenerator;
import feast.proto.core.DataSourceProto;
import feast.proto.core.DataSourceProto.DataFormat.FormatCase;
import feast.proto.core.DataSourceProto.DataSource.SourceType;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  public void shouldErrorOnUnsupportedDataFormats() {

    // Invert the supported formats to get a map of unsupported formats
    Map<SourceType, Set<FormatCase>> unsupportedFormatMap =
        DataSourceValidator.SUPPORTED_FORMATS.entrySet().stream()
            .map(
                es -> {
                  Set<FormatCase> supportedFormats = es.getValue();
                  Set<FormatCase> badFormats =
                      Stream.of(FormatCase.values())
                          .filter(format -> !supportedFormats.contains(format))
                          .collect(Collectors.toSet());
                  return Map.entry(es.getKey(), badFormats);
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Check that validator should reject unsupported formats
    unsupportedFormatMap.forEach(
        (sourceType, badFormats) -> {
          DataSourceProto.DataSource.Builder spec = getTestSpecsMap().get(sourceType).toBuilder();
          badFormats.forEach(
              badFormat -> {
                switch (sourceType) {
                  case BATCH_FILE:
                    spec.setFileOptions(
                        spec.getFileOptions()
                            .toBuilder()
                            .setFileFormat(getTestFormatsMap().get(badFormat)));
                    break;
                  case STREAM_KAFKA:
                    spec.setKafkaOptions(
                        spec.getKafkaOptions()
                            .toBuilder()
                            .setMessageFormat(getTestFormatsMap().get(badFormat)));
                    break;
                  case STREAM_KINESIS:
                    spec.setKinesisOptions(
                        spec.getKinesisOptions()
                            .toBuilder()
                            .setRecordFormat(getTestFormatsMap().get(badFormat)));
                  default:
                    throw new RuntimeException(
                        String.format("Unhandled data source type in test: %s", sourceType));
                }

                boolean hasError = false;
                try {
                  DataSourceValidator.validate(spec.build());
                } catch (UnsupportedOperationException e) {
                  hasError = true;
                }
                assertTrue(hasError);
              });
        });
  }

  private Map<SourceType, DataSourceProto.DataSource> getTestSpecsMap() {
    return Map.of(
        BATCH_FILE,
            DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""),
        BATCH_BIGQUERY,
            DataGenerator.createKafkaDataSourceSpec(
                "localhost:9092", "topic", "class.path", "ts_col"),
        STREAM_KINESIS,
            DataGenerator.createBigQueryDataSourceSpec("project:dataset.table", "ts_col", "dt_col"),
        STREAM_KAFKA,
            DataGenerator.createKinesisDataSourceSpec(
                "ap-nowhere1", "stream", "class.path", "ts_col"));
  }

  private Map<FormatCase, DataSourceProto.DataFormat> getTestFormatsMap() {
    return Map.of(
        PARQUET_FORMAT, DataGenerator.createParquetFormat(),
        PROTO_FORMAT, DataGenerator.createProtoFormat("class.path"),
        AVRO_FORMAT, DataGenerator.createAvroFormat("{}"));
  }
}
