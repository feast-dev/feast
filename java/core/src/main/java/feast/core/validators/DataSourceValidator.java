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

import static feast.core.validators.Matchers.*;
import static feast.proto.core.DataSourceProto.DataSource.SourceType.*;

import feast.proto.core.DataFormatProto.FileFormat;
import feast.proto.core.DataFormatProto.StreamFormat;
import feast.proto.core.DataSourceProto.DataSource;

public class DataSourceValidator {
  /**
   * Validate if the given DataSource protobuf spec is valid.
   *
   * @param spec spec to be validated
   */
  public static void validate(DataSource spec) {
    switch (spec.getType()) {
      case BATCH_FILE:
        FileFormat.FormatCase fileFormat = spec.getFileOptions().getFileFormat().getFormatCase();
        switch (fileFormat) {
          case PARQUET_FORMAT:
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("Unsupported File Format: %s", fileFormat));
        }
        break;

      case BATCH_BIGQUERY:
        checkValidBigQueryTableRef(spec.getBigqueryOptions().getTableRef(), "FeatureTable");
        break;

      case STREAM_KAFKA:
        StreamFormat.FormatCase messageFormat =
            spec.getKafkaOptions().getMessageFormat().getFormatCase();
        switch (messageFormat) {
          case PROTO_FORMAT:
            checkValidClassPath(
                spec.getKafkaOptions().getMessageFormat().getProtoFormat().getClassPath(),
                "FeatureTable");
            break;
          case AVRO_FORMAT:
            break;
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Unsupported Stream Format for Kafka Source Type: %s", messageFormat));
        }
        break;

      case STREAM_KINESIS:
        // Verify tht DataFormat is supported by kinesis data source
        StreamFormat.FormatCase recordFormat =
            spec.getKinesisOptions().getRecordFormat().getFormatCase();
        switch (recordFormat) {
          case PROTO_FORMAT:
            checkValidClassPath(
                spec.getKinesisOptions().getRecordFormat().getProtoFormat().getClassPath(),
                "FeatureTable");
            break;
          case AVRO_FORMAT:
            break;
          default:
            throw new UnsupportedOperationException(
                String.format("Unsupported Stream Format for Kafka Source Type: %s", recordFormat));
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Feature Store Type: %s", spec.getType()));
    }
  }
}
