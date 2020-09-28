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

import feast.core.util.TypeConversion;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.BigQueryOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.FileOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.FileOptions.FileFormat;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.KafkaOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.KinesisOptions;
import feast.proto.core.FeatureSourceProto.FeatureSourceSpec.SourceType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter(AccessLevel.PRIVATE)
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

  // Source type specific options
  // File Options
  @Enumerated(EnumType.STRING)
  @Column(name = "file_format")
  private FileFormat fileFormat;

  @Column(name = "file_url")
  private String fileURL;

  // BigQuery Options
  @Column(name = "bigquery_project_id")
  private String bigQueryProjectId;

  @Column(name = "bigquery_sql_query")
  private String bigQuerySQLQuery;

  // Kafka Options
  @Column(name = "kafka_bootstrap_servers")
  private String kafkaBootstrapServers;

  @Column(name = "kafka_topic")
  private String kafkaTopic;

  @Column(name = "kafka_class_path")
  private String kafkaClassPath;

  // Kinesis Options
  @Column(name = "kinesis_region")
  private String kinesisRegion;

  @Column(name = "kinesis_stream_name")
  private String kinesisStreamName;

  @Column(name = "kinesis_class_path")
  private String kinesisClassPath;

  // Field mapping between sourced fields (key) and feature fields (value).
  // Stored as serialized JSON string.
  @Column(name = "field_mapping", columnDefinition = "text")
  private String fieldMapJSON;

  public FeatureSource() {};

  public FeatureSource(SourceType type) {
    this.type = type;
  }

  /**
   * Construct a FeatureSource from the given Protobuf representation spec
   *
   * @param spec Protobuf representation of Feature source to construct from.
   * @throws IllegalArgumentException when provided with a invalid Protobuf spec
   * @throws UnsupportedOperationException if source type is unsupported.
   */
  public static FeatureSource fromProto(FeatureSourceSpec spec) {
    FeatureSource source = new FeatureSource(spec.getType());
    // Copy source type specific options
    switch (spec.getType()) {
      case BATCH_FILE:
        source.setFileURL(spec.getFileOptions().getFileUrl());
        source.setFileFormat(spec.getFileOptions().getFileFormat());
        break;
      case BATCH_BIGQUERY:
        source.setBigQueryProjectId(spec.getBigqueryOptions().getProjectId());
        source.setBigQuerySQLQuery(spec.getBigqueryOptions().getSqlQuery());
        break;
      case STREAM_KAFKA:
        source.setKafkaBootstrapServers(spec.getKafkaOptions().getBootstrapServers());
        source.setKafkaTopic(spec.getKafkaOptions().getTopic());
        source.setKafkaClassPath(spec.getKafkaOptions().getClassPath());
        break;
      case STREAM_KINESIS:
        source.setKinesisRegion(spec.getKinesisOptions().getRegion());
        source.setKinesisStreamName(spec.getKinesisOptions().getStreamName());
        source.setKinesisClassPath(spec.getKinesisOptions().getClassPath());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Feature Store Type: %s", spec.getType()));
    }

    // Store field mapping as serialised JSON
    source.setFieldMapJSON(TypeConversion.convertMapToJsonString(spec.getFieldMappingMap()));

    return source;
  }

  /** Convert this FeatureSource to its Protobuf representation. */
  public FeatureSourceSpec toProto() {
    FeatureSourceSpec.Builder spec = FeatureSourceSpec.newBuilder();
    spec.setType(getType());

    // Extract source type specific options
    switch (getType()) {
      case BATCH_FILE:
        FileOptions.Builder fileOptions = FileOptions.newBuilder();
        fileOptions.setFileUrl(getFileURL());
        fileOptions.setFileFormat(getFileFormat());
        spec.setFileOptions(fileOptions.build());
        break;
      case BATCH_BIGQUERY:
        BigQueryOptions.Builder bigQueryOptions = BigQueryOptions.newBuilder();
        bigQueryOptions.setProjectId(getBigQueryProjectId());
        bigQueryOptions.setSqlQuery(getBigQuerySQLQuery());
        spec.setBigqueryOptions(bigQueryOptions.build());
        break;
      case STREAM_KAFKA:
        KafkaOptions.Builder kafkaOptions = KafkaOptions.newBuilder();
        kafkaOptions.setBootstrapServers(getKafkaBootstrapServers());
        kafkaOptions.setTopic(getKafkaTopic());
        kafkaOptions.setClassPath(getKafkaClassPath());
        spec.setKafkaOptions(kafkaOptions.build());
        break;
      case STREAM_KINESIS:
        KinesisOptions.Builder kinesisOptions = KinesisOptions.newBuilder();
        kinesisOptions.setRegion(getKinesisRegion());
        kinesisOptions.setStreamName(getKinesisStreamName());
        kinesisOptions.setClassPath(getKinesisClassPath());
        spec.setKinesisOptions(kinesisOptions.build());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Feature Store Type: %s", getType()));
    }

    // Parse field mapping and options from JSON
    spec.putAllFieldMapping(TypeConversion.convertJsonStringToMap(getFieldMapJSON()));

    return spec.build();
  }

  @Override
  public int hashCode() {
    return toProto().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FeatureSource other = (FeatureSource) o;
    return this.toProto().equals(other.toProto());
  }
}
