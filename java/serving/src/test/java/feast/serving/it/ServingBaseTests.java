/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.FieldStatus;
import feast.proto.types.ValueProto;
import feast.serving.util.DataGenerator;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;

abstract class ServingBaseTests extends ServingEnvironment {

  protected ServingAPIProto.GetOnlineFeaturesRequest buildOnlineRequest(int driverId) {
    // getOnlineFeatures Information
    String entityName = "driver_id";

    // Instantiate EntityRows
    Map<String, ValueProto.RepeatedValue> entityRows =
        ImmutableMap.of(
            entityName,
            ValueProto.RepeatedValue.newBuilder()
                .addVal(DataGenerator.createInt64Value(driverId))
                .build());

    ImmutableList<String> featureReferences =
        ImmutableList.of("driver_hourly_stats:conv_rate", "driver_hourly_stats:avg_daily_trips");

    // Build GetOnlineFeaturesRequestV2
    return TestUtils.createOnlineFeatureRequest(featureReferences, entityRows);
  }

  static RegistryProto.Registry registryProto = readLocalRegistry();

  private static RegistryProto.Registry readLocalRegistry() {
    try {
      return RegistryProto.Registry.parseFrom(
          Files.readAllBytes(Paths.get("src/test/resources/docker-compose/feast10/registry.db")));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Test
  public void shouldGetOnlineFeatures() {
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(1005));

    assertEquals(2, featureResponse.getResultsCount());
    assertEquals(1, featureResponse.getResults(0).getValuesCount());

    assertEquals(
        ImmutableList.of("driver_hourly_stats:conv_rate", "driver_hourly_stats:avg_daily_trips"),
        featureResponse.getMetadata().getFeatureNames().getValList());

    for (int featureIdx : List.of(0, 1)) {
      assertEquals(
          List.of(ServingAPIProto.FieldStatus.PRESENT),
          featureResponse.getResults(featureIdx).getStatusesList());
    }

    assertEquals(0.5, featureResponse.getResults(0).getValues(0).getDoubleVal(), 0.0001);
    assertEquals(500, featureResponse.getResults(1).getValues(0).getInt64Val());
  }

  @Test
  public void shouldGetOnlineFeaturesWithOutsideMaxAgeStatus() {
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(1001));

    assertEquals(2, featureResponse.getResultsCount());
    assertEquals(1, featureResponse.getResults(0).getValuesCount());

    for (int featureIdx : List.of(0, 1)) {
      assertEquals(
          FieldStatus.OUTSIDE_MAX_AGE, featureResponse.getResults(featureIdx).getStatuses(0));
    }

    assertEquals(0.1, featureResponse.getResults(0).getValues(0).getDoubleVal(), 0.0001);
    assertEquals(100, featureResponse.getResults(1).getValues(0).getInt64Val());
  }

  @Test
  public void shouldGetOnlineFeaturesWithNotFoundStatus() {
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(-1));

    assertEquals(2, featureResponse.getResultsCount());
    assertEquals(1, featureResponse.getResults(0).getValuesCount());

    for (final int featureIdx : List.of(0, 1)) {
      assertEquals(FieldStatus.NOT_FOUND, featureResponse.getResults(featureIdx).getStatuses(0));
    }
  }

  @Test
  public void shouldRefreshRegistryAndServeNewFeatures() throws InterruptedException {
    updateRegistryFile(
        registryProto
            .toBuilder()
            .addFeatureViews(
                FeatureViewProto.FeatureView.newBuilder()
                    .setSpec(
                        FeatureViewProto.FeatureViewSpec.newBuilder()
                            .setName("new_view")
                            .addEntities("driver_id")
                            .addFeatures(
                                FeatureProto.FeatureSpecV2.newBuilder()
                                    .setName("new_feature")
                                    .setValueType(ValueProto.ValueType.Enum.BOOL))))
            .build());

    ServingAPIProto.GetOnlineFeaturesRequest request = buildOnlineRequest(1005);

    ServingAPIProto.GetOnlineFeaturesRequest requestWithNewFeature =
        request
            .toBuilder()
            .setFeatures(request.getFeatures().toBuilder().addVal("new_view:new_feature"))
            .build();

    await()
        .ignoreException(StatusRuntimeException.class)
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> servingStub.getOnlineFeatures(requestWithNewFeature).getResultsCount(),
            equalTo(3));
  }

  /** https://github.com/feast-dev/feast/issues/2253 */
  @Test
  public void shouldGetOnlineFeaturesWithStringEntity() {
    Map<String, ValueProto.RepeatedValue> entityRows =
        ImmutableMap.of(
            "entity",
            ValueProto.RepeatedValue.newBuilder()
                .addVal(DataGenerator.createStrValue("key-1"))
                .build());

    ImmutableList<String> featureReferences =
        ImmutableList.of("feature_view_0:feature_0", "feature_view_0:feature_1");

    ServingAPIProto.GetOnlineFeaturesRequest req =
        TestUtils.createOnlineFeatureRequest(featureReferences, entityRows);

    ServingAPIProto.GetOnlineFeaturesResponse resp = servingStub.getOnlineFeatures(req);

    for (final int featureIdx : List.of(0, 1)) {
      assertEquals(FieldStatus.PRESENT, resp.getResults(featureIdx).getStatuses(0));
    }
  }

  abstract void updateRegistryFile(RegistryProto.Registry registry);
}
