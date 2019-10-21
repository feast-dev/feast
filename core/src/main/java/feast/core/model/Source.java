package feast.core.model;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import feast.core.SourceProto;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.Source.Builder;
import feast.core.SourceProto.SourceType;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import lombok.Setter;

@Setter
@Entity
@Table(name = "sources")
public class Source {

  private static final Set<String> KAFKA_OPTIONS = Sets.newHashSet("bootstrapServers");

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  @Column(name = "id", updatable = false, nullable = false)
  private Long id;

  // Type of the source. Should map to feast.types.Source.SourceType
  @Column(name = "type", nullable = false)
  private String type;

  // Options for this source
  @Column(name = "options")
  @Lob
  private byte[] options;

  @Column(name = "use_default")
  private boolean useDefault;

  public Source() {
    super();
  }

  public Source(SourceType type, byte[] options) {
    this.type = type.toString();
    this.options = options;
  }

  public static Source fromProto(SourceProto.Source sourceProto) {
    if (sourceProto.equals(SourceProto.Source.getDefaultInstance())) {
      Source source = new Source(SourceType.UNRECOGNIZED,
          KafkaSourceConfig.getDefaultInstance().toByteArray());
      source.setUseDefault(true);
      return source;
    }

    byte[] options;
    switch (sourceProto.getType()) {
      case KAFKA:
        options = sourceProto.getKafkaSourceConfig().toByteArray();
        break;
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException("Unsupported source type. Only [KAFKA] is supported.");
    }
    return new Source(sourceProto.getType(), options);
  }

  public SourceProto.Source toProto() throws InvalidProtocolBufferException {
    Builder builder = SourceProto.Source.newBuilder()
        .setType(SourceType.valueOf(type));
    switch (SourceType.valueOf(type)) {
      case KAFKA:
        KafkaSourceConfig config = KafkaSourceConfig.parseFrom(options);
        return builder.setKafkaSourceConfig(config).build();
      case UNRECOGNIZED:
      default:
        throw new RuntimeException("Unable to convert source to proto");
    }
  }

  /**
   * Get the options for this feature source
   *
   * @return feature source options
   */
  public Message getOptions()
      throws InvalidProtocolBufferException {
    switch (SourceType.valueOf(type)) {
      case KAFKA:
        KafkaSourceConfig config = KafkaSourceConfig.parseFrom(options);
        return config;
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
  public boolean isUseDefault() {
    return useDefault;
  }

  /**
   * Set the topic to the source stream.
   */
  public void setTopic(String topic) throws InvalidProtocolBufferException {
    switch (SourceType.valueOf(type)) {
      case KAFKA:
        KafkaSourceConfig kafkacfg = KafkaSourceConfig.parseFrom(options);
        this.options = kafkacfg.toBuilder().setTopic(topic).build().toByteArray();
      case UNRECOGNIZED:
      default:
        throw new RuntimeException("Unable to convert source to proto");
    }
  }

  public boolean equalTo(Source other) throws InvalidProtocolBufferException {
    if (other.useDefault && useDefault) {
      return true;
    }

    if (!type.equals(other.type)) {
      return false;
    }

    switch (SourceType.valueOf(type)) {
      case KAFKA:
        KafkaSourceConfig kafkaCfg = KafkaSourceConfig.parseFrom(options);
        KafkaSourceConfig otherKafkaCfg = KafkaSourceConfig.parseFrom(other.options);
        return kafkaCfg.getBootstrapServers().equals(otherKafkaCfg.getBootstrapServers());
      case UNRECOGNIZED:
      default:
        return false;
    }
  }
}


