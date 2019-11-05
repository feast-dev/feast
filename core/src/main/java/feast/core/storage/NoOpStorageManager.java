package feast.core.storage;

import feast.specs.FeatureSpecProto;

public class NoOpStorageManager implements StorageManager{

  public static final String TYPE = "noop";

  @Override
  public void registerNewFeature(FeatureSpecProto.FeatureSpec featureSpec) {
  }

}
