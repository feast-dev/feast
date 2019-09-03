package feast.core.model;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.types.ValueProto.ValueType;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "feature_sets")
public class FeatureSet extends AbstractTimestampEntity implements Comparable<FeatureSet> {

  // Id of the featureSet, defined as name:version
  @Id
  @Column(name = "id", nullable = false, unique = true)
  private String id;

  // Name of the featureSet
  @Column(name = "name", nullable = false, unique = true)
  private String name;

  // Version of the featureSet
  @Column(name = "version")
  private int version;

  @OneToMany
  @JoinColumn(name = "entities")
  private List<Field> entities;

  // Features inside this featureSet
  @OneToMany
  @JoinColumn(name = "features")
  private List<Field> features;

  // Source on which feature rows can be found
  @ManyToOne
  @JoinColumn(name = "source")
  private Source source;

  public FeatureSet() {
    super();
  }

  public FeatureSet(String name, int version, List<Field> entities, List<Field> features,
      Source source) {
    this.id = String.format("%s:%s", name, version);
    this.name = name;
    this.version = version;
    this.entities = entities;
    this.features = features;
    this.source = source;
  }

  public static FeatureSet fromProto(FeatureSetSpec featureSetSpec) {
    Source source = Source.fromProto(featureSetSpec.getSource());
    String id = String.format("%s:%d", featureSetSpec.getName(), featureSetSpec.getVersion());
    List<Field> features = new ArrayList<>();
    for (FeatureSpec feature : featureSetSpec.getFeaturesList()) {
      features.add(new Field(id,
          feature.getName(),
          feature.getValueType()));
    }
    List<Field> entities = new ArrayList<>();
    for (EntitySpec entity : featureSetSpec.getEntitiesList()) {
      entities.add(new Field(id,
          entity.getName(),
          entity.getValueType()));
    }

    return new FeatureSet(featureSetSpec.getName(),
        featureSetSpec.getVersion(),
        entities,
        features,
        source);
  }

  public FeatureSetSpec toProto() {
    List<EntitySpec> entitySpecs = new ArrayList<>();
    for (Field entity : entities) {
      entitySpecs.add(EntitySpec.newBuilder()
          .setName(entity.getName())
          .setValueType(ValueType.Enum.valueOf(entity.getType()))
          .build());
    }

    List<FeatureSpec> featureSpecs = new ArrayList<>();
    for (Field feature : features) {
      entitySpecs.add(EntitySpec.newBuilder()
          .setName(feature.getName())
          .setValueType(ValueType.Enum.valueOf(feature.getType()))
          .build());
    }
    return FeatureSetSpec.newBuilder()
        .setName(name)
        .setVersion(version)
        .addAllEntities(entitySpecs)
        .addAllFeatures(featureSpecs)
        .setSource(source.toProto())
        .build();
  }

  @Override
  public int compareTo(FeatureSet o) {
    return Integer.compare(version, o.version);
  }
}
