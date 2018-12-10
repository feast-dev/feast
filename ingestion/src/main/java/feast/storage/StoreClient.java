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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.FeatureRowProto.FeatureRow;

/** Abstraction of Client for Feast Storage. */
public interface StoreClient {

  /** Get a the latest FeatureRow containing features for a single entity id */
  FeatureRow getLatest(String entityName, String entityId, List<FeatureSpec> featureSpecs);

  /** Get a the latest FeatureRow for containing features for each entity id */
  List<FeatureRow> getLatest(
      String entityName, Collection<String> entityIds, List<FeatureSpec> featureSpecs);

  /**
   * Get a the N most recent FeatureRows for single entity id. Each featureRow will contain at least
   * one of the features provided
   */
  List<FeatureRow> getLatest(
      String entityName, String entityId, List<FeatureSpec> featureSpecs, int limit);

  /**
   * Get up to N most recent FeatureRows for each entity id. Each featureRow will contain at least
   * one of the features provided
   * @return A map of feature ids to lists of FeatureRows
   */
  Map<String, List<FeatureRow>> getLatestRange(
      String entityName, Collection<String> entityId, List<FeatureSpec> featureSpecs, int limit, TimestampRange tsRange);
}
