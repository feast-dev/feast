/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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

import com.google.protobuf.TextFormat;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source.Builder;
import feast.proto.core.SourceProto.SourceType;
import io.grpc.Status;
import java.util.Objects;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@AllArgsConstructor
@Table(name = "sources")
public class Source {

  /** Source Id. Internal use only, do not use to identify the source. */
  @Id
  @GeneratedValue
  @Column(name = "pk")
  private Integer id;

  @Deprecated
  @Column(name = "id")
  private String deprecatedId;

  @Deprecated
  @Column(name = "bootstrap_servers")
  private String bootstrapServers;

  @Deprecated
  @Column(name = "topics")
  private String topics;

  /** Type of the source */
  @Enumerated(EnumType.STRING)
  @Column(name = "type", nullable = false)
  private SourceType type;

  /** Configuration object specific to each source type */
  @Column(name = "config")
  private String config;

  @Column(name = "is_default")
  private boolean isDefault;

  public Source() {
    super();
  }

  public String getConfig() {
    if ((config == null || config.isEmpty()) && bootstrapServers != null && topics != null) {
      config =
          KafkaSourceConfig.newBuilder()
              .setBootstrapServers(bootstrapServers)
              .setTopic(topics)
              .build()
              .toString();
    }

    return config;
  }

  /**
   * Construct a source facade object from a given proto object.
   *
   * @param sourceSpec SourceProto.Source object
   * @param isDefault Whether to return the default source object if the source was not defined by
   *     the user
   * @return Source facade object
   */
  public static Source fromProto(SourceProto.Source sourceSpec, boolean isDefault) {

    if (sourceSpec.equals(SourceProto.Source.getDefaultInstance())) {
      Source source = new Source();
      source.setDefault(true);
      return source;
    }

    Source source = new Source();
    source.setType(sourceSpec.getType());

    switch (sourceSpec.getType()) {
      case KAFKA:
        if (sourceSpec.getKafkaSourceConfig().getBootstrapServers().isEmpty()
            || sourceSpec.getKafkaSourceConfig().getTopic().isEmpty()) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  "Unsupported source options. Kafka source requires bootstrap servers and topic to be specified.")
              .asRuntimeException();
        }
        source.setConfig(sourceSpec.getKafkaSourceConfig().toString());
        break;
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unsupported source type. Only [KAFKA] is supported.")
            .asRuntimeException();
    }

    source.setDefault(isDefault);
    return source;
  }

  /**
   * Construct a source facade object from a given proto object.
   *
   * @param sourceSpec SourceProto.Source object
   * @return Source facade object
   */
  public static Source fromProto(SourceProto.Source sourceSpec) {
    return fromProto(sourceSpec, false);
  }

  /**
   * Convert this object to its equivalent proto object.
   *
   * @return SourceProto.Source
   */
  public SourceProto.Source toProto() {
    Builder builder = SourceProto.Source.newBuilder().setType(this.getType());

    switch (this.getType()) {
      case KAFKA:
        KafkaSourceConfig.Builder kafkaSourceConfig = KafkaSourceConfig.newBuilder();
        try {
          com.google.protobuf.TextFormat.getParser().merge(this.getConfig(), kafkaSourceConfig);
        } catch (TextFormat.ParseException e) {
          throw new RuntimeException(
              String.format(
                  "Unable to deserialize source configuration from String to KafkaSourceConfig: %s",
                  this.getConfig()),
              e);
        }
        return builder.setKafkaSourceConfig(kafkaSourceConfig).build();
      case INVALID:
      case UNRECOGNIZED:
      default:
        throw new RuntimeException(
            String.format(
                "Unable to build Source from configuration and type: %s %s",
                this.getConfig(), this.getType()));
    }
  }

  /**
   * Override equality for sources. Sources are compared based on their type and type-specific
   * options.
   *
   * @param other other Source
   * @return boolean equal
   */
  public boolean equalTo(Source other) {
    if ((this.getType() == null || !this.getType().equals(other.getType()))) {
      return false;
    }

    return this.getConfig().equals(other.getConfig());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Source source = (Source) o;
    return this.equalTo(source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getConfig());
  }

  /**
   * Returns the type of this Source in String format
   *
   * @return Source type in String format
   */
  public String getTypeString() {
    return this.getType().getValueDescriptor().getName();
  }
}
