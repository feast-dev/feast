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
import feast.serving.model.Pair;
import feast.specs.FeatureSpecProto.FeatureSpec;
import java.util.List;

/** Abstraction of Feast Storage. */
public interface FeatureStorage {
  /**
   * Get current value of the given feature for the specified entities.
   *
   * <p>It returns the most recent value if the feature is time-series (granularity other than NONE)
   *
   * @param entityName entity name, e.g. 'driver', 'customer', 'area'
   * @param entityIds list of entity id.
   * @param featureSpec feature spec for which the feature should be retrieved.
   * @return list of feature value.
   * @throws FeatureRetrievalException if anything goes wrong during feature retrieval.
   */
  List<FeatureValue> getCurrentFeature(
      String entityName, List<String> entityIds, FeatureSpec featureSpec);
  /**
   * Get current value of several feature for the specified entities.
   *
   * <p>It returns the most recent value if the feature is time-series (granularity other than NONE)
   *
   * @param entityName entity name, e.g. 'driver', 'customer', 'area'
   * @param entityIds list of entity id.
   * @param featureSpecs list of feature spec for which the feature should be retrieved.
   * @return list of feature value.
   * @throws FeatureRetrievalException if anything goes wrong during feature retrieval.
   */
  List<FeatureValue> getCurrentFeatures(
      String entityName, List<String> entityIds, List<FeatureSpec> featureSpecs);

  /**
   * Get N latest value of a feature within a timestamp range.
   *
   * <p>The number of data returned is less than or equal to {@code n}.
   *
   * @param entityName entity name
   * @param entityIds list of entity id that the feature should be retrieved.
   * @param featureSpecAndLimitPair pair between feature spec and the maximum number of value to be
   *     returned.
   * @param tsRange timerange filter.
   * @return map of entity id and the feature values.
   * @throws FeatureRetrievalException if anything goes wrong during feature retrieval.
   */
  List<FeatureValue> getNLatestFeatureWithinTimestampRange(
      String entityName,
      List<String> entityIds,
      Pair<FeatureSpec, Integer> featureSpecAndLimitPair,
      TimestampRange tsRange);

  /**
   * Get N latest value of several features within a timestamp range.
   *
   * <p>The number of data returned is less than or equal to {@code n}.
   *
   * @param entityName entity name
   * @param entityIds list of entity id that the feature should be retrieved.
   * @param featureSpecAndLimitPairs list of pair between feature spec and the maximum number of
   *     value to be returned.
   * @param tsRange timerange filter.
   * @return map of entity id and the feature values.
   * @throws FeatureRetrievalException if anything goes wrong during feature retrieval.
   */
  List<FeatureValue> getNLatestFeaturesWithinTimestampRange(
      String entityName,
      List<String> entityIds,
      List<Pair<FeatureSpec, Integer>> featureSpecAndLimitPairs,
      TimestampRange tsRange);
}
