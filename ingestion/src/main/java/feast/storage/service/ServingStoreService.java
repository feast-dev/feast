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
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;
import feast.storage.ServingStore;

@Slf4j
public class ServingStoreService {
  private static ServiceLoader<ServingStore> serviceLoader = ServiceLoader.load(ServingStore.class);
  private static List<ServingStore> manuallyRegistered = new ArrayList<>();

  static {
    for (ServingStore store : getAll()) {
      log.info("ServingStore type found: " + store.getType());
    }
  }

  public static List<ServingStore> getAll() {
    return Lists.newArrayList(
        Iterators.concat(manuallyRegistered.iterator(), serviceLoader.iterator()));
  }

  /** Get store of the given subclass. */
  public static <T extends ServingStore> T get(Class<T> clazz) {
    for (ServingStore store : getAll()) {
      if (clazz.isInstance(store)) {
        //noinspection unchecked
        return (T) store;
      }
    }
    return null;
  }

  public static void register(ServingStore store) {
    manuallyRegistered.add(store);
  }
}
