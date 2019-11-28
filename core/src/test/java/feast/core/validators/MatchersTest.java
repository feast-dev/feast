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

import com.google.common.base.Strings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MatchersTest {
  @Rule public final ExpectedException exception = ExpectedException.none();

  @Test
  public void checkUpperSnakeCaseShouldPassForLegitUpperSnakeCase() {
    String in = "REDIS_DB";
    checkUpperSnakeCase(in, "someField");
  }

  @Test
  public void checkUpperSnakeCaseShouldPassForLegitUpperSnakeCaseWithNumbers() {
    String in = "REDIS1";
    checkUpperSnakeCase(in, "someField");
  }

  @Test
  public void checkUpperSnakeCaseShouldThrowIllegalArgumentExceptionWithFieldForInvalidString() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        Strings.lenientFormat(
            "invalid value for field %s: %s",
            "someField",
            "argument must be in upper snake case, and cannot include any special characters."));
    String in = "redis";
    checkUpperSnakeCase(in, "someField");
  }

  @Test
  public void checkLowerSnakeCaseShouldPassForLegitLowerSnakeCase() {
    String in = "feature_name_v1";
    checkLowerSnakeCase(in, "someField");
  }

  @Test
  public void checkLowerSnakeCaseShouldThrowIllegalArgumentExceptionWithFieldForInvalidString() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage(
        Strings.lenientFormat(
            "invalid value for field %s: %s",
            "someField",
            "argument must be in lower snake case, and cannot include any special characters."));
    String in = "Invalid_feature name";
    checkLowerSnakeCase(in, "someField");
  }
}
