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
package feast.core.validators;

import static feast.proto.types.ValueProto.ValueType.Enum.*;

import feast.common.it.DataGenerator;
import feast.proto.core.FeatureProto.FeatureSpecV2;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class FeatureTableValidatorTest {

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfLabelsHasEmptyKey() {
    Map<String, String> badLabels = Map.of("", "empty");
    FeatureTableSpec badSpec = getTestSpec().toBuilder().putAllLabels(badLabels).build();
    FeatureTableValidator.validateSpec(badSpec);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfFeaturesLabelsHasEmptyKey() {
    Map<String, String> badLabels = Map.of("", "empty");

    List<FeatureSpecV2> badFeatureSpecs =
        getTestSpec().getFeaturesList().stream()
            .map(featureSpec -> featureSpec.toBuilder().putAllLabels(badLabels).build())
            .collect(Collectors.toList());
    FeatureTableSpec badSpec = getTestSpec().toBuilder().addAllFeatures(badFeatureSpecs).build();
    FeatureTableValidator.validateSpec(badSpec);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfUsedReservedName() {
    FeatureTableSpec badSpec =
        getTestSpec().toBuilder().addAllEntities(FeatureTableValidator.RESERVED_NAMES).build();
    FeatureTableValidator.validateSpec(badSpec);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfNamesUsedNotUnique() {
    FeatureTableSpec badSpec =
        DataGenerator.createFeatureTableSpec(
            "driver", List.of("region"), Map.of("region", STRING), 3600, Map.of());
    FeatureTableValidator.validateSpec(badSpec);
  }

  private FeatureTableSpec getTestSpec() {
    return DataGenerator.createFeatureTableSpec(
        "driver", List.of("driver_id"), Map.of("n_drivers", INT64), 3600, Map.of());
  }
}
