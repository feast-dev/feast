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

package feast.ingestion.service;

import java.io.Serializable;
import java.util.Map;
import lombok.AllArgsConstructor;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;

/**
 * Abstraction of service which provides {@link EntitySpec}, {@link FeatureSpec}, and {@link
 * StorageSpec}.
 */
public interface SpecService {
  /**
   * Get a map of {@link EntitySpec} from Core API, given a collection of entityId.
   *
   * @param entityIds collection of entityId to retrieve.
   * @return map of {@link EntitySpec}, where the key is entity name.
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds);

  /**
   * Get a map of {@link FeatureSpec} from Core API, given a collection of featureId.
   *
   * @param featureIds collection of entityId to retrieve.
   * @return map of {@link FeatureSpec}, where the key is feature id.
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds);

  /**
   * Get map of {@link StorageSpec} from Core API, given a collection of storageId.
   *
   * @param storageIds collection of storageId to retrieve.
   * @return map of {@link StorageSpec}, where the key is storage id.
   * @throws SpecRetrievalException if any error happens during retrieval
   */
  Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds);

  interface Builder extends Serializable {
    SpecService build();
  }

  @AllArgsConstructor
  class UnsupportedBuilder implements Builder {
    private String message;

    @Override
    public SpecService build() {
      throw new UnsupportedOperationException(message);
    }
  }
}
