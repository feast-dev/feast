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
package feast.serving.it;

import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.util.*;

public class TestUtils {

  public static ServingServiceGrpc.ServingServiceBlockingStub getServingServiceStub(
      boolean isSecure, int feastServingPort, Map<String, String> options) {
    Channel secureChannel =
        ManagedChannelBuilder.forAddress("localhost", feastServingPort).usePlaintext().build();
    return ServingServiceGrpc.newBlockingStub(secureChannel);
  }

  public static GetOnlineFeaturesRequestV2 createOnlineFeatureRequest(
      String projectName,
      List<FeatureReferenceV2> featureReferences,
      List<GetOnlineFeaturesRequestV2.EntityRow> entityRows) {
    return GetOnlineFeaturesRequestV2.newBuilder()
        .setProject(projectName)
        .addAllFeatures(featureReferences)
        .addAllEntityRows(entityRows)
        .build();
  }
}
