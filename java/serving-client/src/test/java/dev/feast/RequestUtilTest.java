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
package dev.feast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

class RequestUtilTest {

  private static Stream<Arguments> provideValidFeatureRefs() {
    return Stream.of(
        Arguments.of(
            Arrays.asList("driver:driver_id"),
            Arrays.asList(
                FeatureReferenceV2.newBuilder()
                    .setFeatureViewName("driver")
                    .setFeatureName("driver_id")
                    .build())));
  }

  @ParameterizedTest
  @MethodSource("provideValidFeatureRefs")
  void createFeatureRefs_ShouldReturnFeaturesForValidFeatureRefs(
      List<String> input, List<FeatureReferenceV2> expected) {
    List<FeatureReferenceV2> actual = RequestUtil.createFeatureRefs(input);
    // Order of the actual and expected FeatureTables do no not matter
    actual.sort(Comparator.comparing(FeatureReferenceV2::getFeatureName));
    expected.sort(Comparator.comparing(FeatureReferenceV2::getFeatureName));
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      String expectedString = TextFormat.printer().printToString(expected.get(i));
      String actualString = TextFormat.printer().printToString(actual.get(i));
      assertEquals(expectedString, actualString);
    }
  }

  @ParameterizedTest
  @MethodSource("provideValidFeatureRefs")
  void renderFeatureRef_ShouldReturnFeatureRefString(
      List<String> expected, List<FeatureReferenceV2> input) {
    input = input.stream().map(ref -> ref.toBuilder().build()).collect(Collectors.toList());
    List<String> actual =
        input.stream()
            .map(ref -> String.format("%s:%s", ref.getFeatureViewName(), ref.getFeatureName()))
            .collect(Collectors.toList());
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  private static Stream<Arguments> provideInvalidFeatureRefs() {
    return Stream.of(Arguments.of(ImmutableList.of("project/feature")));
  }

  private static Stream<Arguments> provideMissingFeatureTableFeatureRefs() {
    return Stream.of(Arguments.of(ImmutableList.of("feature")));
  }

  @ParameterizedTest
  @MethodSource("provideInvalidFeatureRefs")
  void createFeatureRefs_ShouldThrowExceptionForProjectInFeatureRefs(List<String> input) {
    assertThrows(IllegalArgumentException.class, () -> RequestUtil.createFeatureRefs(input));
  }

  @ParameterizedTest
  @MethodSource("provideMissingFeatureTableFeatureRefs")
  void createFeatureRefs_ShouldThrowExceptionForMissingFeatureTableInFeatureRefs(
      List<String> input) {
    assertThrows(IllegalArgumentException.class, () -> RequestUtil.createFeatureRefs(input));
  }

  @ParameterizedTest
  @NullSource
  void createFeatureRefs_ShouldThrowExceptionForNullFeatureRefs(List<String> input) {
    assertThrows(IllegalArgumentException.class, () -> RequestUtil.createFeatureRefs(input));
  }
}
