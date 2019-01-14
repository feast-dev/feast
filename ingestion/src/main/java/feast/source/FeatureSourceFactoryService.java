package feast.source;

import avro.shaded.com.google.common.collect.Lists;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FeatureSourceFactoryService {

  private static ServiceLoader<FeatureSourceFactory> serviceLoader = ServiceLoader
      .load(FeatureSourceFactory.class);
  private static List<FeatureSourceFactory> manuallyRegistered = new ArrayList<>();

  static {
    for (FeatureSourceFactory source : getAll()) {
      log.info("FeatureSourceFactory type found: " + source.getType());
    }
  }

  public static List<FeatureSourceFactory> getAll() {
    return Lists.newArrayList(
        Iterators.concat(manuallyRegistered.iterator(), serviceLoader.iterator()));
  }

  /**
   * Get store of the given subclass.
   */
  public static <T extends FeatureSourceFactory> T get(Class<T> clazz) {
    for (FeatureSourceFactory store : getAll()) {
      if (clazz.isInstance(store)) {
        //noinspection unchecked
        return (T) store;
      }
    }
    return null;
  }

  public static void register(FeatureSourceFactory store) {
    manuallyRegistered.add(store);
  }
}
