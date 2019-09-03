package feast.core.model;

import com.google.common.collect.Sets;
import feast.core.SourceProto;
import feast.core.SourceProto.Source.SourceType;
import feast.core.util.TypeConversion;
import java.util.Map;
import java.util.Set;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Setter;

@Setter
@Entity
@Table(name = "sources")
public class Source {

  private static final Set<String> KAFKA_OPTIONS = Sets.newHashSet("bootstrapServers", "topics");

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  @Column(name = "id", updatable = false, nullable = false)
  private Long id;

  // Type of the source. Should map to feast.types.Source.SourceType
  @Column(name = "type", nullable = false)
  private String type;

  // Options for this source
  @Column(name = "options")
  private String options;

  public Source() {
    super();
  }

  public Source(SourceType type, Map<String, String> options) {
    this.type = type.toString();
    this.options = TypeConversion.convertMapToJsonString(options);
  }

  public static Source fromProto(SourceProto.Source sourceProto) throws IllegalArgumentException {
    switch (sourceProto.getType()) {
      case KAFKA:
        if (!sourceProto.getOptionsMap().keySet().containsAll(KAFKA_OPTIONS)) {
          throw new IllegalArgumentException(
              "Invalid source options. For SourceType.KAFKA, the following key value options are accepted:\n"
                  + "  // - bootstrapServers: [comma delimited value of host[:port]]\n"
                  + "  // - topics: [comma delimited value of topic names]");
        }
        break;
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException("Unsupported source type. Only [KAFKA] is supported.");
    }
    return new Source(sourceProto.getType(), sourceProto.getOptionsMap());
  }

  public SourceProto.Source toProto() {
    return SourceProto.Source.newBuilder()
        .setType(SourceType.valueOf(type))
        .putAllOptions(getOptions())
        .build();
  }

  /**
   * Get the options for this feature source as a map
   *
   * @return map of feature source options
   */
  public Map<String, String> getOptions() {
    return TypeConversion.convertJsonStringToMap(this.options);
  }
}


