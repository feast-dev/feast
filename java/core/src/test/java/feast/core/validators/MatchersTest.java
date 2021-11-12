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
package feast.core.validators;

import static feast.core.validators.Matchers.checkLowerSnakeCase;
import static feast.core.validators.Matchers.checkUpperSnakeCase;
import static feast.core.validators.Matchers.checkValidClassPath;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.base.Strings;
import org.junit.Test;

public class MatchersTest {

  @Test
  public void checkUpperSnakeCaseShouldPassForLegitUpperSnakeCase() {
    String in = "REDIS_DB";
    checkUpperSnakeCase(in, "featuretable");
  }

  @Test
  public void checkUpperSnakeCaseShouldPassForLegitUpperSnakeCaseWithNumbers() {
    String in = "REDIS1";
    checkUpperSnakeCase(in, "featuretable");
  }

  @Test
  public void checkUpperSnakeCaseShouldThrowIllegalArgumentExceptionWithFieldForInvalidString() {
    String in = "redis";
    assertThrows(
        IllegalArgumentException.class,
        () -> checkUpperSnakeCase(in, "featuretable"),
        Strings.lenientFormat(
            "invalid value for %s resource, %s: %s",
            "featuretable",
            "redis",
            "argument must be in upper snake case, and cannot include any special characters."));
  }

  @Test
  public void checkLowerSnakeCaseShouldPassForLegitLowerSnakeCase() {
    String in = "feature_name_v1";
    checkLowerSnakeCase(in, "feature");
  }

  @Test
  public void checkLowerSnakeCaseShouldThrowIllegalArgumentExceptionWithFieldForInvalidString() {
    String in = "Invalid_feature name";
    assertThrows(
        IllegalArgumentException.class,
        () -> checkLowerSnakeCase(in, "feature"),
        Strings.lenientFormat(
            "invalid value for %s resource, %s: %s",
            "feature",
            "Invalid_feature name",
            "argument must be in lower snake case, and cannot include any special characters."));
  }

  @Test
  public void checkValidClassPathSuccess() {
    checkValidClassPath("com.example.foo", "FeatureTable");
    checkValidClassPath("com.example", "FeatureTable");
  }

  @Test
  public void checkValidClassPathEmpty() {
    assertThrows(IllegalArgumentException.class, () -> checkValidClassPath("", "FeatureTable"));
  }

  @Test
  public void checkValidClassPathDigits() {
    assertThrows(IllegalArgumentException.class, () -> checkValidClassPath("123", "FeatureTable"));
  }
}
