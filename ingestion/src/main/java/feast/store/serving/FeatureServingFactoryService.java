/*
 * Copyright 2018 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.store.serving;

import avro.shaded.com.google.common.collect.Lists;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Service class for fetching all the FeatureServingFactory instances available
 */
@Slf4j
public class FeatureServingFactoryService {

  private static ServiceLoader<FeatureServingFactory> serviceLoader = ServiceLoader
      .load(FeatureServingFactory.class);
  private static List<FeatureServingFactory> manuallyRegistered = new ArrayList<>();

  static {
    for (FeatureServingFactory store : getAll()) {
      log.info("FeatureServingFactory type found: " + store.getType());
    }
  }

  public static List<FeatureServingFactory> getAll() {
    return Lists.newArrayList(
        Iterators.concat(manuallyRegistered.iterator(), serviceLoader.iterator()));
  }

  /**
   * Get store of the given subclass.
   */
  public static <T extends FeatureServingFactory> T get(Class<T> clazz) {
    for (FeatureServingFactory store : getAll()) {
      if (clazz.isInstance(store)) {
        //noinspection unchecked
        return (T) store;
      }
    }
    return null;
  }

  public static void register(FeatureServingFactory store) {
    manuallyRegistered.add(store);
  }
}
