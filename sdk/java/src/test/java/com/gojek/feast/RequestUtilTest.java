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
import feast.serving.ServingAPIProto.FeatureReference;
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
            Collections.singletonList("driver_project/driver_id"),
            Collections.singletonList(
                FeatureReference.newBuilder()
                    .setProject("driver_project")
                    .setName("driver_id")
                    .build())),
        Arguments.of(
            Arrays.asList("driver_project/driver_id", "driver_project/driver_name"),
            Arrays.asList(
                FeatureReference.newBuilder()
                    .setProject("driver_project")
                    .setName("driver_id")
                    .build(),
                FeatureReference.newBuilder()
                    .setProject("driver_project")
                    .setName("driver_name")
                    .build())),
        Arguments.of(
            Arrays.asList(
                "driver_project/driver_id",
                "driver_project/driver_name",
                "booking_project/driver_name"),
            Arrays.asList(
                FeatureReference.newBuilder()
                    .setProject("driver_project")
                    .setName("driver_id")
                    .build(),
                FeatureReference.newBuilder()
                    .setProject("driver_project")
                    .setName("driver_name")
                    .build(),
                FeatureReference.newBuilder()
                    .setProject("booking_project")
                    .setName("driver_name")
                    .build())));
  }

  @ParameterizedTest
  @MethodSource("provideValidFeatureIds")
  void createFeatureSets_ShouldReturnFeatureSetsForValidFeatureIds(
      List<String> input, List<FeatureReference> expected) {
    List<FeatureReference> actual = RequestUtil.createFeatureRefs(input, "my-project");
    // Order of the actual and expected featureSets do no not matter
    actual.sort(Comparator.comparing(FeatureReference::getName));
    expected.sort(Comparator.comparing(FeatureReference::getName));
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      String expectedString = TextFormat.printer().printToString(expected.get(i));
      String actualString = TextFormat.printer().printToString(actual.get(i));
      assertEquals(expectedString, actualString);
    }
  }

  private static Stream<Arguments> provideInvalidFeatureRefs() {
    return Stream.of(
        Arguments.of(Collections.singletonList("/noproject")),
        Arguments.of(Collections.singletonList("")));
  }

  @ParameterizedTest
  @MethodSource("provideInvalidFeatureRefs")
  void createFeatureSets_ShouldThrowExceptionForInvalidFeatureRefs(List<String> input) {
    assertThrows(
        IllegalArgumentException.class, () -> RequestUtil.createFeatureRefs(input, "my-project"));
  }

  @ParameterizedTest
  @NullSource
  void createFeatureSets_ShouldThrowExceptionForNullFeatureRefs(List<String> input) {
    assertThrows(
        IllegalArgumentException.class, () -> RequestUtil.createFeatureRefs(input, "my-project"));
  }
}
