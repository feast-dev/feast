/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.serving.service;

import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesRequest;
import feast.proto.serving.TransformationServiceAPIProto.TransformFeaturesResponse;
import feast.proto.serving.TransformationServiceAPIProto.ValueType;
import feast.proto.types.ValueProto;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

public interface TransformationService {
  /**
   * Apply on demand transformations for the specified ODFVs.
   *
   * @param transformFeaturesRequest proto containing the ODFV references and necessary data
   * @return a proto object containing the response
   */
  TransformFeaturesResponse transformFeatures(TransformFeaturesRequest transformFeaturesRequest);

  /**
   * Extract the list of on demand feature inputs from a list of ODFV references.
   *
   * @param onDemandFeatureReferences list of ODFV references to be parsed
   * @return list of on demand feature inputs
   */
  List<ServingAPIProto.FeatureReferenceV2> extractOnDemandFeaturesDependencies(
      List<ServingAPIProto.FeatureReferenceV2> onDemandFeatureReferences);

  /**
   * Process a response from the feature transformation server by augmenting the given lists of
   * field maps and status maps with the correct fields from the response.
   *
   * @param transformFeaturesResponse response to be processed
   * @param onDemandFeatureViewName name of ODFV to which the response corresponds
   * @param onDemandFeatureStringReferences set of all ODFV references that should be kept
   * @param responseBuilder {@link ServingAPIProto.GetOnlineFeaturesResponse.Builder}
   */
  void processTransformFeaturesResponse(
      TransformFeaturesResponse transformFeaturesResponse,
      String onDemandFeatureViewName,
      Set<String> onDemandFeatureStringReferences,
      ServingAPIProto.GetOnlineFeaturesResponse.Builder responseBuilder);

  /**
   * Serialize data into Arrow IPC format, to be sent to the Python feature transformation server.
   *
   * @param values list of field maps to be serialized
   * @return the data packaged into a ValueType proto object
   */
  ValueType serializeValuesIntoArrowIPC(List<Pair<String, List<ValueProto.Value>>> values);
}
