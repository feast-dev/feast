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
package com.gojek.feast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.TextFormat;
import feast.serving.ServingAPIProto.FeatureSetRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

class RequestUtilTest {

  private static Stream<Arguments> provideValidFeatureIds() {
    return Stream.of(
        Arguments.of(
            Collections.singletonList("driver:1:driver_id"),
            Collections.singletonList(
                FeatureSetRequest.newBuilder()
                    .setName("driver")
                    .setVersion(1)
                    .addFeatureNames("driver_id"))),
        Arguments.of(
            Arrays.asList("driver:1:driver_id", "driver:1:driver_name"),
            Collections.singletonList(
                FeatureSetRequest.newBuilder()
                    .setName("driver")
                    .setVersion(1)
                    .addAllFeatureNames(Arrays.asList("driver_id", "driver_name"))
                    .build())),
        Arguments.of(
            Arrays.asList("driver:1:driver_id", "driver:1:driver_name", "booking:2:booking_id"),
            Arrays.asList(
                FeatureSetRequest.newBuilder()
                    .setName("driver")
                    .setVersion(1)
                    .addAllFeatureNames(Arrays.asList("driver_id", "driver_name"))
                    .build(),
                FeatureSetRequest.newBuilder()
                    .setName("booking")
                    .setVersion(2)
                    .addFeatureNames("booking_id")
                    .build())));
  }

  @ParameterizedTest
  @MethodSource("provideValidFeatureIds")
  void createFeatureSets_ShouldReturnFeatureSetsForValidFeatureIds(
      List<String> input, List<FeatureSetRequest> expected) {
    List<FeatureSetRequest> actual = RequestUtil.createFeatureSets(input);
    // Order of the actual and expected featureSets do no not matter
    actual.sort(Comparator.comparing(FeatureSetRequest::getName));
    expected.sort(Comparator.comparing(FeatureSetRequest::getName));
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      String expectedString = TextFormat.printer().printToString(expected.get(i));
      String actualString = TextFormat.printer().printToString(actual.get(i));
      assertEquals(expectedString, actualString);
    }
  }

  private static Stream<Arguments> provideInvalidFeatureIds() {
    return Stream.of(
        Arguments.of(Collections.singletonList("feature_set_only")),
        Arguments.of(Collections.singletonList("missing:feature_name")),
        Arguments.of(Collections.singletonList("invalid:version:value")),
        Arguments.of(Collections.singletonList("")));
  }

  @ParameterizedTest
  @MethodSource("provideInvalidFeatureIds")
  void createFeatureSets_ShouldThrowExceptionForInvalidFeatureIds(List<String> input) {
    assertThrows(IllegalArgumentException.class, () -> RequestUtil.createFeatureSets(input));
  }

  @ParameterizedTest
  @NullSource
  void createFeatureSets_ShouldThrowExceptionForNullFeatureIds(List<String> input) {
    assertThrows(IllegalArgumentException.class, () -> RequestUtil.createFeatureSets(input));
  }
}
