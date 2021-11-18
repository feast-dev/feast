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
package feast.common.validators;

import java.util.Arrays;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/** Validates whether a string value is found within a collection. */
public class OneOfStringValidator implements ConstraintValidator<OneOfStrings, String> {

  /** Values that are permitted for a specific instance of this validator */
  String[] allowedValues;

  /**
   * Initialize the OneOfStringValidator with a collection of allowed String values.
   *
   * @param constraintAnnotation constraint annotation
   */
  @Override
  public void initialize(OneOfStrings constraintAnnotation) {
    allowedValues = constraintAnnotation.value();
  }

  /**
   * Validates whether a string value is found within the collection defined in the annotation.
   *
   * @param value String value that should be validated
   * @param context Provides contextual data and operation when applying a given constraint
   *     validator
   * @return Boolean value indicating whether the string is found within the allowed values.
   */
  @Override
  public boolean isValid(String value, ConstraintValidatorContext context) {
    return Arrays.asList(allowedValues).contains(value);
  }
}
