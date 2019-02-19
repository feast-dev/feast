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

package feast.serving.util;

import com.google.common.base.Strings;
import feast.serving.ServingAPIProto.QueryFeaturesRequest;

public class RequestHelper {
  private RequestHelper() {}

  public static void validateRequest(QueryFeaturesRequest request) {
    // entity name shall present
    if (Strings.isNullOrEmpty(request.getEntityName())) {
      throw new IllegalArgumentException("entity name must be set");
    }

    // entity id list shall not be empty
    if (request.getEntityIdList().size() <= 0) {
      throw new IllegalArgumentException("entity ID must be provided");
    }

    // feature IDs shall not be empty
    if (request.getFeatureIdCount() <= 0) {
      throw new IllegalArgumentException("feature id must be provided");
    }

    // feature id in each request detail shall have same entity name
    String entityName = request.getEntityName();
    for (String featureId : request.getFeatureIdList()) {
      if (!featureId.substring(0, featureId.indexOf(".")).equals(entityName)) {
        throw new IllegalArgumentException(
            "entity name of all feature ID in request details must be: " + entityName);
      }
    }
  }
}
