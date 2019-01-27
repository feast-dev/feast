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

package feast.storage.service;

import avro.shaded.com.google.common.collect.Lists;
import com.google.common.collect.Iterators;
import feast.storage.FeatureServingStoreFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Service class for fetching all the FeatureServingStoreFactory instances available
 */
@Slf4j
public class FeatureServingStoreFactoryService {
  private static ServiceLoader<FeatureServingStoreFactory> serviceLoader = ServiceLoader.load(FeatureServingStoreFactory.class);
  private static List<FeatureServingStoreFactory> manuallyRegistered = new ArrayList<>();

  static {
    for (FeatureServingStoreFactory store : getAll()) {
      log.info("FeatureServingStoreFactory type found: " + store.getType());
    }
  }

  public static List<FeatureServingStoreFactory> getAll() {
    return Lists.newArrayList(
        Iterators.concat(manuallyRegistered.iterator(), serviceLoader.iterator()));
  }

  /** Get store of the given subclass. */
  public static <T extends FeatureServingStoreFactory> T get(Class<T> clazz) {
    for (FeatureServingStoreFactory store : getAll()) {
      if (clazz.isInstance(store)) {
        //noinspection unchecked
        return (T) store;
      }
    }
    return null;
  }

  public static void register(FeatureServingStoreFactory store) {
    manuallyRegistered.add(store);
  }
}
