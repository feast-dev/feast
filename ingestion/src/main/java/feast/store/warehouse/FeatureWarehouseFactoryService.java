// /*
//  * Copyright 2018 The Feast Authors
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     https://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  *
//  */
//
// package feast.store.warehouse;
//
// import com.google.common.collect.Iterators;
// import com.google.common.collect.Lists;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.ServiceLoader;
// import lombok.extern.slf4j.Slf4j;
//
// /**
//  * Service class for fetching all the FeatureWarehouseFactory instances available
//  */
// @Slf4j
// public class FeatureWarehouseFactoryService {
//   private static ServiceLoader<FeatureWarehouseFactory> serviceLoader =
//       ServiceLoader.load(FeatureWarehouseFactory.class);
//   private static List<FeatureWarehouseFactory> manuallyRegistered = new ArrayList<>();
//
//   static {
//     for (FeatureWarehouseFactory store : getAll()) {
//       log.info("FeatureWarehouseFactory type found: " + store.getType());
//     }
// }
//
//   public static List<FeatureWarehouseFactory> getAll() {
//     return Lists.newArrayList(
//         Iterators.concat(manuallyRegistered.iterator(), serviceLoader.iterator()));
//   }
//
//   /** Get store of the given subclass. */
//   public static <T extends FeatureWarehouseFactory> T get(Class<T> clazz) {
//     for (FeatureWarehouseFactory store : getAll()) {
//       if (clazz.isInstance(store)) {
//         //noinspection unchecked
//         return (T) store;
//       }
//     }
//     return null;
//   }
//
//   public static void register(FeatureWarehouseFactory store) {
//     manuallyRegistered.add(store);
//   }
//   }
