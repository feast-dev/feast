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

package feast.storage;

import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Collection;
import java.util.List;

/**
 * Abstraction of Client for Feast Serving Store
 */
public interface FeatureServingStoreClient {

  /**
   * Get a the latest FeatureRow containing features for a single entity id
   */
  FeatureRow get(String entityName, String entityId, List<FeatureSpec> featureSpecs);

  /**
   * Get a the latest FeatureRow for containing features for each entity id
   */
  List<FeatureRow> get(
      String entityName, Collection<String> entityIds, List<FeatureSpec> featureSpecs);

}