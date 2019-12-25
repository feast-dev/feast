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

import java.util.regex.Pattern;

public class Matchers {

  private static Pattern UPPER_SNAKE_CASE_REGEX = Pattern.compile("^[A-Z0-9]+(_[A-Z0-9]+)*$");
  private static Pattern LOWER_SNAKE_CASE_REGEX = Pattern.compile("^[a-z0-9]+(_[a-z0-9]+)*$");
  private static Pattern VALID_CHARACTERS_REGEX = Pattern.compile("^[a-zA-Z0-9\\-_]*$");
  private static Pattern VALID_CHARACTERS_REGEX_WITH_ASTERISK_WILDCARD =
      Pattern.compile("^[a-zA-Z0-9\\-_*]*$");

  private static String ERROR_MESSAGE_TEMPLATE = "invalid value for field %s: %s";

  public static void checkUpperSnakeCase(String input, String fieldName)
      throws IllegalArgumentException {
    if (!UPPER_SNAKE_CASE_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              fieldName,
              "argument must be in upper snake case, and cannot include any special characters."));
    }
  }

  public static void checkLowerSnakeCase(String input, String fieldName)
      throws IllegalArgumentException {
    if (!LOWER_SNAKE_CASE_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              fieldName,
              "argument must be in lower snake case, and cannot include any special characters."));
    }
  }

  public static void checkValidCharacters(String input, String fieldName)
      throws IllegalArgumentException {
    if (!VALID_CHARACTERS_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              fieldName,
              "argument must only contain alphanumeric characters, dashes and underscores."));
    }
  }

  public static void checkValidCharactersAllowAsterisk(String input, String fieldName)
      throws IllegalArgumentException {
    if (!VALID_CHARACTERS_REGEX_WITH_ASTERISK_WILDCARD.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              fieldName,
              "argument must only contain alphanumeric characters, dashes, underscores, or an asterisk."));
    }
  }
}
