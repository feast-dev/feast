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

import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import feast.core.SourceProto;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source.Builder;
import feast.core.SourceProto.SourceType;
import io.grpc.Status;
import java.util.Objects;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Setter;

@Setter
@Entity
@Table(name = "sources")
public class Source {

  private static final Set<String> KAFKA_OPTIONS = Sets.newHashSet("bootstrapServers");

  @Id
  @Column(name = "id", updatable = false, nullable = false)
  private String id;

  // Type of the source. Should map to feast.types.Source.SourceType
  @Column(name = "type", nullable = false)
  private String type;

  // Bootstrap servers, comma delimited. Used by kafka sources.
  @Column(name = "bootstrap_servers")
  private String bootstrapServers;

  // Topics to listen to, comma delimited. Used by kafka sources.
  @Column(name = "topics")
  private String topics;

  @Column(name = "is_default")
  private boolean isDefault;

  public Source() {
    super();
  }

  public Source(SourceType type, KafkaSourceConfig config, boolean isDefault) {
    if (config.getBootstrapServers().isEmpty() || config.getTopic().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Unsupported source options. Kafka source requires bootstrap servers and topic to be specified.")
          .asRuntimeException();
    }
    this.type = type.toString();
    this.bootstrapServers = config.getBootstrapServers();
    this.topics = config.getTopic();
    this.isDefault = isDefault;
    this.id = generateId();
  }

  /**
   * Construct a source facade object from a given proto object.
   *
   * @param sourceProto SourceProto.Source object
   * @return Source facade object
   */
  public static Source fromProto(SourceProto.Source sourceProto) {
    if (sourceProto.equals(SourceProto.Source.getDefaultInstance())) {
      Source source = new Source();
      source.isDefault = true;
      return source;
    }

    switch (sourceProto.getType()) {
      case KAFKA:
        return new Source(sourceProto.getType(), sourceProto.getKafkaSourceConfig(), false);
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unsupported source type. Only [KAFKA] is supported.")
            .asRuntimeException();
    }
  }

  /**
   * Convert this object to its equivalent proto object.
   *
   * @return SourceProto.Source
   */
  public SourceProto.Source toProto() {
    Builder builder = SourceProto.Source.newBuilder().setType(SourceType.valueOf(type));
    switch (SourceType.valueOf(type)) {
      case KAFKA:
        KafkaSourceConfig config =
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers(bootstrapServers)
                .setTopic(topics)
                .build();
        return builder.setKafkaSourceConfig(config).build();
      case UNRECOGNIZED:
      default:
        throw new RuntimeException("Unable to convert source to proto");
    }
  }

  /**
   * Get the id for this feature source
   *
   * @return feature source id in the format TYPE/options
   */
  public String getId() {
    return id;
  }

  /**
   * Get the options for this feature source
   *
   * @return feature source options
   */
  public Message getOptions() {
    switch (SourceType.valueOf(type)) {
      case KAFKA:
        return KafkaSourceConfig.newBuilder()
            .setBootstrapServers(bootstrapServers)
            .setTopic(topics)
            .build();
      case UNRECOGNIZED:
      default:
        throw new RuntimeException("Unable to convert source to proto");
    }
  }

  /**
   * Get the type of source.
   *
   * @return SourceType of this feature source
   */
  public SourceType getType() {
    return SourceType.valueOf(type);
  }

  /**
   * Indicate whether to use the system defaults or not.
   *
   * @return boolean indicating whether this feature set source uses defaults.
   */
  public boolean isDefault() {
    return isDefault;
  }

  /**
   * Override equality for sources. isDefault is always compared first; if both sources are using
   * the default feature source, they will be equal. If not they will be compared based on their
   * type-specific options.
   *
   * @param other other Source
   * @return boolean equal
   */
  public boolean equalTo(Source other) {
    if (other.isDefault && isDefault || (type == null && other.type == null)) {
      return true;
    }

    if ((type == null || !type.equals(other.type))) {
      return false;
    }

    switch (SourceType.valueOf(type)) {
      case KAFKA:
        return bootstrapServers.equals(other.bootstrapServers) && topics.equals(other.topics);
      case UNRECOGNIZED:
      default:
        return false;
    }
  }

  private String generateId() {
    switch (SourceType.valueOf(type)) {
      case KAFKA:
        return String.format("KAFKA/%s/%s", bootstrapServers, topics);
      default:
        // should not occur
        return "";
    }
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
    return id.equals(source.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
