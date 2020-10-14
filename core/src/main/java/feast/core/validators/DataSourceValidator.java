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
import static feast.proto.core.DataSourceProto.DataFormat.FormatCase.*;
import static feast.proto.core.DataSourceProto.DataSource.SourceType.*;

import feast.proto.core.DataSourceProto.DataFormat.FormatCase;
import feast.proto.core.DataSourceProto.DataSource;
import feast.proto.core.DataSourceProto.DataSource.SourceType;
import java.util.Map;
import java.util.Set;

public class DataSourceValidator {
  // Map data source type to its supported data formats
  public static final Map<SourceType, Set<FormatCase>> SUPPORTED_FORMATS =
      Map.of(
          BATCH_FILE, Set.of(PARQUET_FORMAT),
          STREAM_KAFKA, Set.of(PROTO_FORMAT),
          STREAM_KINESIS, Set.of(PROTO_FORMAT));

  /** Validate if the given DataSource protobuf spec is valid. */
  public static void validate(DataSource spec) {
    switch (spec.getType()) {
      case BATCH_FILE:
        // Verify that DataFormat is supported by file source
        FormatCase fileFormat = spec.getFileOptions().getFileFormat().getFormatCase();
        validateFormat(spec.getType(), fileFormat);
        break;
      case BATCH_BIGQUERY:
        checkValidBigQueryTableRef(spec.getBigqueryOptions().getTableRef(), "FeatureTable");
        break;
      case STREAM_KAFKA:
        // Verify that DataFormat is supported by kafka data source
        FormatCase messageFormat = spec.getKafkaOptions().getMessageFormat().getFormatCase();
        validateFormat(spec.getType(), messageFormat);
        break;
      case STREAM_KINESIS:
        // Verify that DataFormat is supported by kinesis data source
        FormatCase recordFormat = spec.getKinesisOptions().getRecordFormat().getFormatCase();
        validateFormat(spec.getType(), recordFormat);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Feature Store Type: %s", spec.getType()));
    }
  }

  /** Valdiates if the given data format is valid for the given data source type. */
  private static void validateFormat(SourceType sourceType, FormatCase format) {
    if (!SUPPORTED_FORMATS.get(sourceType).contains(format)) {
      throw new UnsupportedOperationException(
          String.format("Unsupported Data Format for %s DataSource: %s", sourceType, format));
    }
  }
}
