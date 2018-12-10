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

package feast.serving.service;

import feast.serving.exception.SpecRetrievalException;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.Map;

/**
 * Abstraction of service which provides {@link EntitySpec}, {@link FeatureSpec}, and {@link
 * StorageSpec}.
 */
public interface SpecStorage {
  /**
   * Get a map of {@link EntitySpec} from Core API, given a collection of entityId.
   *
   * @param entityIds collection of entityId to retrieve.
   * @return map of {@link EntitySpec}, where the key is entity name.
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds);

  /**
   * Get all {@link EntitySpec} from Core API.
   *
   * @return map of {@link EntitySpec}, where the key is the entity name.
   */
  Map<String, EntitySpec> getAllEntitySpecs();

  /**
   * Get a map of {@link FeatureSpec} from Core API, given a collection of featureId.
   *
   * @param featureIds collection of entityId to retrieve.
   * @return map of {@link FeatureSpec}, where the key is feature id.
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds);

  /**
   * Get all {@link FeatureSpec} available in Core API.
   *
   * @return map of {@link FeatureSpec}, where the key is feature id.
   */
  Map<String, FeatureSpec> getAllFeatureSpecs();

  /**
   * Get map of {@link StorageSpec} from Core API, given a collection of storageId.
   *
   * @param storageIds collection of storageId to retrieve.
   * @return map of {@link StorageSpec}, where the key is storage id.
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds);

  /**
   * Get all {@link StorageSpec} from Core API.
   *
   * @return map of {@link StorageSpec}, where the key is storage id.
   */
  Map<String, StorageSpec> getAllStorageSpecs();

  /**
   * Check whether connection to spec storage is ready.
   *
   * @return return true if it is ready. Otherwise, return false.
   */
  boolean isConnected();
}
