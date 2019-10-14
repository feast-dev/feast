package feast.core.stream;

import feast.core.SourceProto.SourceType;
import feast.core.model.FeatureSet;
import feast.core.model.Source;

public interface FeatureStream {

  /**
   * Gets the type of feature stream
   * @return type of feature stream
   */
  SourceType getType();

  /**
   * Provisions a sink for the feature producer to write to. For the given topic name.
   *
   *
   */
  Source provision(FeatureSet featureSet) throws RuntimeException;
}
