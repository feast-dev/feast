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

package feast.serving.service.spec;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import java.util.List;
import java.util.Map;

public interface SpecService {

  /**
   * Get backing store information
   *
   * @param id storeId, name of the store
   * @return {@link Store}
   */
  Store getStoreDetails(String id);

  /**
   * Get a map of {@link FeatureSetSpec} of a list of subscription, where the featureSetId
   * (e.g. feature_set_name:1) is the key
   *
   * @return Map of featureSetId and FeatureSetSpec as a key:value pair
   */
  Map<String, FeatureSetSpec> getFeatureSetSpecs(List<Subscription> subscriptions);

  /**
   * Get a specific featureSetSpec where the featureSetId is equal to the id provided
   * @param id featureSetId of the desired featureSet
   * @return FeatureSetSpec of the desired featureSet
   */
  FeatureSetSpec getFeatureSetSpec(String id);

  /**
   * Check whether connection to core service is ready
   *
   * @return return true if it is ready. Otherwise, return false
   */
  boolean isConnected();

}
