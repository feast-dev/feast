/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
 */
package feast.serving.util;

import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import io.grpc.Status;
import java.util.Set;
import java.util.stream.Collectors;

public class RequestHelper {

  public static void validateOnlineRequest(GetOnlineFeaturesRequest request) {
    // EntityDataSetRow shall not be empty
    if (request.getEntityRowsCount() <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Entity value must be provided")
          .asRuntimeException();
    }
  }

  public static void validateBatchRequest(GetBatchFeaturesRequest getFeaturesRequest) {
    if (!getFeaturesRequest.hasDatasetSource()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Dataset source must be provided")
          .asRuntimeException();
    }

    if (!getFeaturesRequest.getDatasetSource().hasFileSource()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Dataset source must be provided: only file source supported")
          .asRuntimeException();
    }

    Set<String> uniqueFeatureNames =
        getFeaturesRequest.getFeaturesList().stream()
            .map(FeatureReference::getName)
            .collect(Collectors.toSet());
    if (uniqueFeatureNames.size() != getFeaturesRequest.getFeaturesList().size()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Feature names must be unique within the request")
          .asRuntimeException();
    }
  }
}
