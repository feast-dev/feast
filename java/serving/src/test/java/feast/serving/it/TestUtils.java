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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto;
import feast.serving.config.ApplicationProperties;
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

  public static GetOnlineFeaturesRequest createOnlineFeatureRequest(
      List<String> featureReferences, Map<String, ValueProto.RepeatedValue> entityRows) {
    return createOnlineFeatureRequest(featureReferences, entityRows, new HashMap<>());
  }

  public static GetOnlineFeaturesRequest createOnlineFeatureRequest(
      List<String> featureReferences,
      Map<String, ValueProto.RepeatedValue> entityRows,
      Map<String, ValueProto.RepeatedValue> requestContext) {
    return GetOnlineFeaturesRequest.newBuilder()
        .setFeatures(ServingAPIProto.FeatureList.newBuilder().addAllVal(featureReferences))
        .putAllEntities(entityRows)
        .putAllRequestContext(requestContext)
        .build();
  }

  public static GetOnlineFeaturesRequest createOnlineFeatureRequest(
      String featureService, Map<String, ValueProto.RepeatedValue> entityRows) {
    return createOnlineFeatureRequest(featureService, entityRows, new HashMap<>());
  }

  public static GetOnlineFeaturesRequest createOnlineFeatureRequest(
      String featureService,
      Map<String, ValueProto.RepeatedValue> entityRows,
      Map<String, ValueProto.RepeatedValue> requestContext) {
    return GetOnlineFeaturesRequest.newBuilder()
        .setFeatureService(featureService)
        .putAllEntities(entityRows)
        .putAllRequestContext(requestContext)
        .build();
  }

  public static ApplicationProperties.FeastProperties createBasicFeastProperties(
      String redisHost, Integer redisPort) {
    final ApplicationProperties.FeastProperties feastProperties =
        new ApplicationProperties.FeastProperties();
    feastProperties.setRegistry("src/test/resources/docker-compose/feast10/registry.db");
    feastProperties.setRegistryRefreshInterval(1);

    feastProperties.setActiveStore("online");
    feastProperties.setProject("feast_project");

    feastProperties.setStores(
        ImmutableList.of(
            new ApplicationProperties.Store(
                "online",
                "REDIS",
                ImmutableMap.of("host", redisHost, "port", redisPort.toString()))));

    return feastProperties;
  }
}
