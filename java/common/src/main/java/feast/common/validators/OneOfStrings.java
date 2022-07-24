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

import java.lang.annotation.*;
import javax.validation.Constraint;
import javax.validation.Payload;

/**
 * Annotation for String "one of" validation. Allows for the definition of a collection through an
 * annotation. The collection is used to test values defined in the object.
 */
@Target({
  ElementType.METHOD,
  ElementType.FIELD,
  ElementType.ANNOTATION_TYPE,
  ElementType.CONSTRUCTOR,
  ElementType.PARAMETER
})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Constraint(validatedBy = OneOfStringValidator.class)
public @interface OneOfStrings {
  /** @return Default error message that is returned if the incorrect value is set */
  String message() default "Field value must be one of the following: {value}";

  /** @return Allows for the specification of validation groups to which this constraint belongs. */
  Class<?>[] groups() default {};

  /**
   * @return An attribute payload that can be used to assign custom payload objects to a constraint.
   */
  Class<? extends Payload>[] payload() default {};

  /** @return Default value that is returned if no allowed values are configured */
  String[] value() default {};
}
