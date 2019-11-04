package feast.ingestion.values;

import feast.types.ValueProto.ValueType;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Field class represents {@link feast.types.FieldProto.Field} but without value.
 *
 * <p>The use for this class is mainly for validating the Fields in FeatureRow.
 */
@DefaultCoder(AvroCoder.class)
public class Field implements Serializable {
  private final String name;
  private final ValueType.Enum type;

  public Field(String name, ValueType.Enum type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public ValueType.Enum getType() {
    return type;
  }
}
