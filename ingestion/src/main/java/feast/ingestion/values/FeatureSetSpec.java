package feast.ingestion.values;

import static feast.ingestion.utils.SpecUtil.getFieldsByName;

import feast.core.FeatureSetProto;
import java.io.Serializable;
import java.util.Map;

/**
 * This class represents {@link feast.core.FeatureSetProto.FeatureSetSpec} but contains fields
 * directly accessible by name for feature validation purposes.
 *
 * <p>The use for this class is mainly for validating the Fields in FeatureRow.
 */
public class FeatureSetSpec implements Serializable {
  private final String id;

  private final Map<String, Field> fields;

  public FeatureSetSpec(FeatureSetProto.FeatureSetSpec featureSetSpec) {
    this.id = String.format("%s:%d", featureSetSpec.getName(), featureSetSpec.getVersion());
    this.fields = getFieldsByName(featureSetSpec);
  }

  public String getId() {
    return id;
  }

  public Field getField(String fieldName) {
    return fields.getOrDefault(fieldName, null);
  }
}
