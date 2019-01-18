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

import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.exception.FeatureRetrievalException;
import feast.serving.model.FeatureValue;
import feast.specs.FeatureSpecProto.FeatureSpec;
import java.util.Collection;
import java.util.List;

/** Abstraction of Feast Storage. */
public interface FeatureStorage {
  /**
   * Get current value of several feature for the specified entities.
   *
   * @param entityName entity name, e.g. 'driver', 'customer', 'area'
   * @param entityIds list of entity id.
   * @param featureSpecs list of feature spec for which the feature should be retrieved. * @param
   * @param tsRange allowed event timestamp of the feature to be returned. Pass null to not filter *
   *     by time.
   * @return list of feature value.
   * @throws FeatureRetrievalException if anything goes wrong during feature retrieval.
   */
  List<FeatureValue> getFeature(
      String entityName,
      Collection<String> entityIds,
      Collection<FeatureSpec> featureSpecs,
      TimestampRange tsRange);
}
