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
// package feast.store.errors;
//
// import com.google.common.collect.Iterators;
// import com.google.common.collect.Lists;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.ServiceLoader;
// import lombok.extern.slf4j.Slf4j;
//
// /**
//  * Service class for fetching all the FeatureErrorsFactory instances available
//  */
// @Slf4j
// public class FeatureErrorsFactoryService {
//
//   private static ServiceLoader<FeatureErrorsFactory> serviceLoader = ServiceLoader
//       .load(FeatureErrorsFactory.class);
//   private static List<FeatureErrorsFactory> manuallyRegistered = new ArrayList<>();
//
//   static {
//     for (FeatureErrorsFactory store : getAll()) {
//       log.info("FeatureErrorsFactory type found: " + store.getType());
//     }
//   }
//
//   public static List<FeatureErrorsFactory> getAll() {
//     return Lists.newArrayList(
//         Iterators.concat(manuallyRegistered.iterator(), serviceLoader.iterator()));
//   }
//
//   /**
//    * Get store of the given subclass.
//    */
//   public static <T extends FeatureErrorsFactory> T get(Class<T> clazz) {
//     for (FeatureErrorsFactory store : getAll()) {
//       if (clazz.isInstance(store)) {
//         //noinspection unchecked
//         return (T) store;
//       }
//     }
//     return null;
//   }
//
//   public static void register(FeatureErrorsFactory store) {
//     manuallyRegistered.add(store);
//   }
// }
