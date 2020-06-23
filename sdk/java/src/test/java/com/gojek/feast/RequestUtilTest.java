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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import feast.common.models.Feature;
import feast.proto.serving.ServingAPIProto.FeatureReference;
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
            Arrays.asList("driver:driver_id", "driver_id"),
            Arrays.asList(
                FeatureReference.newBuilder().setFeatureSet("driver").setName("driver_id").build(),
                FeatureReference.newBuilder().setName("driver_id").build())));
  }

  @ParameterizedTest
  @MethodSource("provideValidFeatureRefs")
  void createFeatureSets_ShouldReturnFeatureSetsForValidFeatureRefs(
      List<String> input, List<FeatureReference> expected) {
    List<FeatureReference> actual = RequestUtil.createFeatureRefs(input);
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

  @ParameterizedTest
  @MethodSource("provideValidFeatureRefs")
  void renderFeatureRef_ShouldReturnFeatureRefString(
      List<String> expected, List<FeatureReference> input) {
    input =
        input.stream()
            .map(ref -> ref.toBuilder().clearProject().build())
            .collect(Collectors.toList());
    List<String> actual =
        input.stream().map(ref -> Feature.getFeatureStringRef(ref)).collect(Collectors.toList());
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  private static Stream<Arguments> provideInvalidFeatureRefs() {
    return Stream.of(Arguments.of(ImmutableList.of("project/feature", "")));
  }

  @ParameterizedTest
  @MethodSource("provideInvalidFeatureRefs")
  void createFeatureSets_ShouldThrowExceptionForInvalidFeatureRefs(List<String> input) {
    assertThrows(IllegalArgumentException.class, () -> RequestUtil.createFeatureRefs(input));
  }

  @ParameterizedTest
  @NullSource
  void createFeatureSets_ShouldThrowExceptionForNullFeatureRefs(List<String> input) {
    assertThrows(IllegalArgumentException.class, () -> RequestUtil.createFeatureRefs(input));
  }
}
