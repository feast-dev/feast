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

import static feast.proto.core.FeatureSourceProto.FeatureSource.SourceType.*;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.proto.core.FeatureSourceProto;
import feast.proto.core.FeatureSourceProto.FeatureSource.BigQueryOptions;
import feast.proto.core.FeatureSourceProto.FeatureSource.FileOptions;
import feast.proto.core.FeatureSourceProto.FeatureSource.KafkaOptions;
import feast.proto.core.FeatureSourceProto.FeatureSource.KinesisOptions;
import feast.proto.core.FeatureSourceProto.FeatureSource.SourceType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Getter;

@Entity
@Getter
@Table(name = "feature_sources")
public class FeatureSource {
  @Column(name = "id")
  @Id
  @GeneratedValue
  private long id;

  /** Type of this feature batch source */
  @Enumerated(EnumType.STRING)
  @Column(name = "type", nullable = false)
  private SourceType type;

  /** Source type specific configuration options. Stored as Protobuf encoded as JSON string. */
  @Column(name = "options", nullable = false)
  private String optionsJSON;

  private FeatureSource(SourceType type, String optionsJSON) {
    this.type = type;
    this.optionsJSON = optionsJSON;
  }

  /**
   * Construct a FeatureSource from the given Protobuf representation spec
   *
   * @param spec Protobuf representation of Feature source to construct from.
   * @throws InvalidProtocolBufferException when provided with a invalid Protobuf spec
   * @throws UnsupportedOperationException if source type is unsupported.
   */
  public static FeatureSource fromProto(FeatureSourceProto.FeatureSource spec)
      throws InvalidProtocolBufferException {
    if (spec.getType().equals(SourceType.INVALID)) {
      throw new InvalidProtocolBufferException("Missing Feature Store type: Type unset");
    }

    // Serialize options as string by converting to JSON
    JsonFormat.Printer jsonPrinter = JsonFormat.printer();
    String optionsJSON = null;
    switch (spec.getType()) {
      case BATCH_FILE:
        optionsJSON = jsonPrinter.print(spec.getFileOptions());
        break;
      case BATCH_BIGQUERY:
        optionsJSON = jsonPrinter.print(spec.getBigqueryOptions());
        break;
      case STREAM_KAFKA:
        optionsJSON = jsonPrinter.print(spec.getKafkaOptions());
        break;
      case STREAM_KINESIS:
        optionsJSON = jsonPrinter.print(spec.getKinesisOptions());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Feature Store Type: %s", spec.getType()));
    }

    return new FeatureSource(spec.getType(), optionsJSON);
  }

  /**
   * Convert this FeatureSource to its Protobuf representation.
   *
   * @return Converted Protobuf representation.
   */
  public FeatureSourceProto.FeatureSource toProto() {
    FeatureSourceProto.FeatureSource.Builder spec = FeatureSourceProto.FeatureSource.newBuilder();
    spec.setType(getType());

    // Parse options from stored options JSON
    JsonFormat.Parser jsonParser = JsonFormat.parser();
    try {
      switch (getType()) {
        case BATCH_FILE:
          FileOptions.Builder fileOptions = FileOptions.newBuilder();
          jsonParser.merge(getOptionsJSON(), fileOptions);
          spec.setFileOptions(fileOptions.build());
          break;
        case BATCH_BIGQUERY:
          BigQueryOptions.Builder bigQueryOptions = BigQueryOptions.newBuilder();
          jsonParser.merge(getOptionsJSON(), bigQueryOptions);
          spec.setBigqueryOptions(bigQueryOptions.build());
          break;
        case STREAM_KAFKA:
          KafkaOptions.Builder kafkaOptions = KafkaOptions.newBuilder();
          jsonParser.merge(getOptionsJSON(), kafkaOptions);
          spec.setKafkaOptions(kafkaOptions.build());
          break;
        case STREAM_KINESIS:
          KinesisOptions.Builder kinesisOptions = KinesisOptions.newBuilder();
          jsonParser.merge(getOptionsJSON(), kinesisOptions);
          spec.setKinesisOptions(kinesisOptions.build());
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported Feature Store Type: %s", getType()));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Unexpected exception when parsing FeatureSource options", e);
    }

    return spec.build();
  }
}
