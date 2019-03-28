/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.options;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

public class OptionsParser {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Validator validator;

  static {
    try (ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory()) {
      validator = validatorFactory.getValidator();
    }
  }

  /**
   * Return a json schema string representing an options class for error messages
   */
  static <T extends Options> String getJsonSchema(Class<T> optionsClass) {
    JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
    JsonSchema schema = null;
    try {
      schema = schemaGen.generateSchema(optionsClass);
      schema.setId(null); // clear the ID as it's visual noise
      return mapper.writer().forType(JsonSchema.class).writeValueAsString(schema);
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * Construct a class from string options and validate with any javax validation annotations
   */
  public static <T extends Options> T parse(Map<String, String> optionsMap, Class<T> clazz) {
    List<String> messages = Lists.newArrayList();
    T options;
    try {
      options = mapper.convertValue(optionsMap, clazz);
    } catch (IllegalArgumentException e) {
      messages.add("Expecting options convertible to schema: " + getJsonSchema(clazz));
      try {
        messages.add("Got " + mapper.writer().writeValueAsString(optionsMap));
      } catch (JsonProcessingException ee) {
        //
      }
      throw new IllegalArgumentException(String.join(", ", messages), e);
    }
    Set<ConstraintViolation<T>> violations = validator.validate(options);
    if (violations.size() > 0) {
      messages.add("Expecting options convertible to schema: " + getJsonSchema(clazz));
      for (ConstraintViolation<T> violation : violations) {
        messages.add(
            String.format(
                "property \"%s\" %s", violation.getPropertyPath(), violation.getMessage()));
      }
      throw new IllegalArgumentException(String.join(", ", messages));
    }
    return options;
  }
}
