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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.util.TypeConversion;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.BigQueryOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.FileOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.KafkaOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.KinesisOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.SourceType;
import java.util.Objects;
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

  // Type of this Feature Source
  @Enumerated(EnumType.STRING)
  @Column(name = "type", nullable = false)
  private SourceType type;

  // Source type specific configuration options stored as Protobuf encoded as JSON string
  @Column(name = "options", nullable = false)
  private String optionsJSON;

  // Field mapping between sourced fields (key) and feature fields (value).
  // Stored as serialized JSON string.
  @Column(name = "field_mapping", columnDefinition = "text")
  private String fieldMapJSON;

  private FeatureSource(SourceType type, String optionsJSON, String fieldMapJSON) {
    this.type = type;
    this.optionsJSON = optionsJSON;
    this.fieldMapJSON = fieldMapJSON;
  }

  /**
   * Construct a FeatureSource from the given Protobuf representation spec
   *
   * @param spec Protobuf representation of Feature source to construct from.
   * @throws IllegalArgumentException when provided with a invalid Protobuf spec
   * @throws UnsupportedOperationException if source type is unsupported.
   */
  public static FeatureSource fromProto(FeatureSourceSpec spec) {
    if (spec.getType().equals(SourceType.INVALID)) {
      throw new IllegalArgumentException("Missing Feature Store type: Type unset");
    }

    // Serialize options and field mapping as string by converting to JSON
    String fieldMapJSON = TypeConversion.convertMapToJsonString(spec.getFieldMappingMap());

    JsonFormat.Printer jsonPrinter = JsonFormat.printer();
    String optionsJSON = null;
    try {
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
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Unexpected error when converting options to JSON:", e);
    }

    return new FeatureSource(spec.getType(), optionsJSON, fieldMapJSON);
  }

  /** Convert this FeatureSource to its Protobuf representation. */
  public FeatureSourceSpec toProto() {
    FeatureSourceSpec.Builder spec = FeatureSourceSpec.newBuilder();
    spec.setType(getType());

    // Parse field mapping and options from JSON
    spec.putAllFieldMapping(TypeConversion.convertJsonStringToMap(getFieldMapJSON()));

    // Contruct JSON parrser - ignore unknown fields required to maintain compatiblity when removing fields
    JsonFormat.Parser jsonParser = JsonFormat.parser().ignoringUnknownFields();
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

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getType(), getOptionsJSON());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FeatureSource feature = (FeatureSource) o;
    return getType().equals(feature.getType()) && getOptionsJSON().equals(feature.getOptionsJSON());
  }
}
