package feast.source;

import feast.specs.ImportSpecProto.ImportSpec;

/**
 * A FeatureSourceFactory creates FeatureSource instances, which can read FeatureRow messages from a
 * source location.
 */
public interface FeatureSourceFactory {

  String getType();

  FeatureSource create(ImportSpec importSpec);
}
