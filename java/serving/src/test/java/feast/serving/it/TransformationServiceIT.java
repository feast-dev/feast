/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2022 The Feast Authors
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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.serving.config.ApplicationProperties;
import feast.serving.util.DataGenerator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TransformationServiceIT extends ServingEnvironment {
  @Override
  ApplicationProperties.FeastProperties createFeastProperties() {
    ApplicationProperties.FeastProperties feastProperties =
        TestUtils.createBasicFeastProperties(
            environment.getServiceHost("redis", 6379), environment.getServicePort("redis", 6379));
    feastProperties.setTransformationServiceEndpoint(
        String.format(
            "%s:%d",
            environment.getServiceHost("feast", 8080), environment.getServicePort("feast", 8080)));
    return feastProperties;
  }

  private ServingAPIProto.GetOnlineFeaturesRequest buildOnlineRequest(
      int driverId, boolean transformedFeaturesOnly) {
    Map<String, ValueProto.RepeatedValue> entityRows =
        ImmutableMap.of(
            "driver_id",
            ValueProto.RepeatedValue.newBuilder()
                .addVal(DataGenerator.createInt64Value(driverId))
                .build());

    Map<String, ValueProto.RepeatedValue> requestContext =
        ImmutableMap.of(
            "val_to_add",
            ValueProto.RepeatedValue.newBuilder().addVal(DataGenerator.createInt64Value(3)).build(),
            "val_to_add_2",
            ValueProto.RepeatedValue.newBuilder()
                .addVal(DataGenerator.createInt64Value(5))
                .build());

    List<String> featureReferences =
        Lists.newArrayList(
            "transformed_conv_rate:conv_rate_plus_val1",
            "transformed_conv_rate:conv_rate_plus_val2");

    if (!transformedFeaturesOnly) {
      featureReferences.add("driver_hourly_stats:conv_rate");
    }

    return TestUtils.createOnlineFeatureRequest(featureReferences, entityRows, requestContext);
  }

  @Test
  public void shouldCalculateOnDemandFeatures() {
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(1005, false));

    for (int featureIdx : List.of(0, 1, 2)) {
      assertEquals(
          List.of(ServingAPIProto.FieldStatus.PRESENT),
          featureResponse.getResults(featureIdx).getStatusesList());
    }

    // conv_rate
    assertEquals(0.5, featureResponse.getResults(0).getValues(0).getDoubleVal(), 0.0001);
    // conv_rate + val_to_add (3.0)
    assertEquals(3.5, featureResponse.getResults(1).getValues(0).getDoubleVal(), 0.0001);
    // conv_rate + val_to_add_2 (5.0)
    assertEquals(5.5, featureResponse.getResults(2).getValues(0).getDoubleVal(), 0.0001);
  }

  @Test
  public void shouldCorrectlyFetchDependantFeatures() {
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeatures(buildOnlineRequest(1005, true));

    // conv_rate + val_to_add (3.0)
    assertEquals(3.5, featureResponse.getResults(0).getValues(0).getDoubleVal(), 0.0001);
    // conv_rate + val_to_add_2 (5.0)
    assertEquals(5.5, featureResponse.getResults(1).getValues(0).getDoubleVal(), 0.0001);
  }
}
