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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.types.ValueProto;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

public class RequestUtilTest {

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

  @Test
  public void objectToValueTest() {
    assertEquals(RequestUtil.objectToValue(42).getInt32Val(), 42);
    assertEquals(RequestUtil.objectToValue(42L).getInt64Val(), 42L);
    assertEquals(RequestUtil.objectToValue(3.14f).getFloatVal(), 3.14f);
    assertEquals(RequestUtil.objectToValue(3.14).getDoubleVal(), 3.14);
    assertEquals(RequestUtil.objectToValue("test").getStringVal(), "test");
    byte[] bytes = "test".getBytes();
    assertArrayEquals(RequestUtil.objectToValue(bytes).getBytesVal().toByteArray(), bytes);
    assertTrue(RequestUtil.objectToValue(true).getBoolVal());
    Long timestampSeconds = 1751920985L;
    Instant instant = Instant.ofEpochMilli(timestampSeconds * 1000);
    assertEquals(
        RequestUtil.objectToValue(LocalDateTime.ofInstant(instant, ZoneId.of("UTC")))
            .getUnixTimestampVal(),
        timestampSeconds);
    assertEquals(
        RequestUtil.objectToValue(
                OffsetDateTime.ofInstant(instant, ZoneId.of("America/Los_Angeles")))
            .getUnixTimestampVal(),
        timestampSeconds);
    assertEquals(RequestUtil.objectToValue(instant).getUnixTimestampVal(), timestampSeconds);
    assertEquals(RequestUtil.objectToValue(null).getNullVal(), ValueProto.Null.NULL);
    assertEquals(
        RequestUtil.objectToValue(Arrays.asList(1, 2, 3)).getInt32ListVal().getValList(),
        Arrays.asList(1, 2, 3));
    assertEquals(
        RequestUtil.objectToValue(Arrays.asList(1L, 2L, 3L)).getInt64ListVal().getValList(),
        Arrays.asList(1L, 2L, 3L));
    assertEquals(
        RequestUtil.objectToValue(Arrays.asList(3.14f, 2.71f)).getFloatListVal().getValList(),
        Arrays.asList(3.14f, 2.71f));
    assertEquals(
        RequestUtil.objectToValue(Arrays.asList(1.0, 2.0, 3.0)).getDoubleListVal().getValList(),
        Arrays.asList(1.0, 2.0, 3.0));
    assertEquals(
        RequestUtil.objectToValue(Arrays.asList("a", "b", "c")).getStringListVal().getValList(),
        Arrays.asList("a", "b", "c"));
    assertArrayEquals(
        RequestUtil.objectToValue(Arrays.asList(bytes, bytes))
            .getBytesListVal()
            .getValList()
            .get(0)
            .toByteArray(),
        bytes);
    assertEquals(
        RequestUtil.objectToValue(Arrays.asList(true, false)).getBoolListVal().getValList(),
        Arrays.asList(true, false));
    Long timestampSeconds2 = 1751920986L;
    Instant instant2 = Instant.ofEpochMilli(timestampSeconds2 * 1000);
    assertEquals(
        RequestUtil.objectToValue(Arrays.asList(instant, instant2))
            .getUnixTimestampListVal()
            .getValList(),
        Arrays.asList(timestampSeconds, timestampSeconds2));
    assertEquals(
        RequestUtil.objectToValue(
                Arrays.asList(
                    LocalDateTime.ofInstant(instant, ZoneId.of("UTC")),
                    LocalDateTime.ofInstant(instant2, ZoneId.of("UTC"))))
            .getUnixTimestampListVal()
            .getValList(),
        Arrays.asList(timestampSeconds, timestampSeconds2));
    assertEquals(
        RequestUtil.objectToValue(
                Arrays.asList(
                    OffsetDateTime.ofInstant(instant, ZoneId.of("America/Los_Angeles")),
                    OffsetDateTime.ofInstant(instant2, ZoneId.of("America/Los_Angeles"))))
            .getUnixTimestampListVal()
            .getValList(),
        Arrays.asList(timestampSeconds, timestampSeconds2));
  }

  @Test
  public void objectToValue_ShouldThrowExceptionForUnsupportedType() {
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> RequestUtil.objectToValue(new Object()));
    assertEquals("Unsupported type: Object", exception.getMessage());
  }

  @Test
  public void objectToValue_ShouldThrowExceptionForEmptyList() {
    List<Object> emptyList = Arrays.asList();
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> RequestUtil.objectToValue(emptyList));
    assertEquals("Unsupported empty list type", exception.getMessage());
  }

  @Test
  public void objectToValue_ShouldThrowExceptionForUnsupportedListType() {
    List<Object> unsupportedList = Arrays.asList(new Object(), new Object());
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> RequestUtil.objectToValue(unsupportedList));
    assertEquals("Unsupported list type: Object", exception.getMessage());

    List<Object> mixedList = Arrays.asList(1, "test", 3.14);
    exception =
        assertThrows(IllegalArgumentException.class, () -> RequestUtil.objectToValue(mixedList));
    assertEquals(
        "Unknown list type, error during casting: class java.lang.String cannot be cast to class java.lang.Integer (java.lang.String and java.lang.Integer are in module java.base of loader 'bootstrap')",
        exception.getMessage());
  }
}
