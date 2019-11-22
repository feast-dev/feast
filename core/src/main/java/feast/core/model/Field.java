package feast.core.model;

import feast.types.ValueProto.ValueType;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "fields")
public class Field {

  // Id of the field, defined as featureSetId.name
  @Id
  @Column(name = "id", nullable = false, unique = true)
  private String id;

  // FeatureSet this feature belongs to
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "feature_set_id")
  private FeatureSet featureSet;

  // Name of the feature
  @Column(name = "name", nullable = false)
  private String name;

  // Type of the feature, should correspond with feast.types.ValueType
  @Column(name = "type", nullable = false)
  private String type;

  public Field() {
    super();
  }

  public Field(String featureSetId, String name, ValueType.Enum type) {
    FeatureSet featureSet = new FeatureSet();
    featureSet.setId(featureSetId);
    this.featureSet = featureSet;
    this.id = String.format("%s:%s", featureSetId, name);
    this.name = name;
    this.type = type.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Field field = (Field) o;
    return name.equals(field.getName()) && type.equals(field.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), id, featureSet, name, type);
  }
}
