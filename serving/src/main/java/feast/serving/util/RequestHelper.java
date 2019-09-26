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

import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;

public class RequestHelper {

  private RequestHelper() {
  }

  public static void validateRequest(GetFeaturesRequest request) {
    // EntityDataSetRow shall not be empty
    if (request.getEntityDataset().getEntityDatasetRowsCount() <= 0) {
      throw new IllegalArgumentException("Entity value must be provided");
    }

    // Value list size in EntityDataSetRow shall be the same as the size of fieldNames
    // First entity value will always be timestamp in EntityDataSetRow
    int fieldNameCount = request.getEntityDataset().getEntityNamesCount();
    for (EntityDatasetRow edsr : request.getEntityDataset().getEntityDatasetRowsList()) {
      if (edsr.getEntityIdsCount() != fieldNameCount) {
        throw new IllegalArgumentException("Size mismatch between fieldNames and its values");
      }
    }
  }
}