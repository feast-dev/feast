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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class FeastOptionsParserTest {

  @Test
  public void getJsonSchema() {
    String jsonSchema =
        "{\"type\":\"object\",\"properties\":{\"foo\":{\"type\":\"string\"},\"bar\":{\"type\":\"integer\"}}}";
    assertEquals(jsonSchema, OptionsParser.getJsonSchema(TestType1.class));
  }

  @Test
  public void parseOptions() {
    TestType1 options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder().put("foo", "x").put("bar", "1").build(),
            TestType1.class);
    assertEquals("x", options.foo);
    assertEquals(1, options.bar);
  }

  @Test
  public void parseOptionsMissingOption() {
    TestType1 options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder().put("bar", "1").build(), TestType1.class);
    assertNull(options.foo);
    assertEquals(1, options.bar);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseOptionsUnknownField() {
    OptionsParser.parse(
        ImmutableMap.<String, String>builder().put("foo", "x").put("biz", "1").build(),
        TestType1.class);
  }

  @Test
  public void parseOptionsCustomProps() {
    TestType2 options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("test.foo", "x")
                .put("test.bar", "1")
                .build(),
            TestType2.class);
    assertEquals("x", options.foo);
    assertEquals(1, options.bar);
  }

  @Test
  public void parseOptionsWithValidation() {
    TestType3 options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder().put("foo", "x").put("bar", "1").build(),
            TestType3.class);
    assertEquals("x", options.foo);
    assertEquals(1, options.bar);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseOptionsWithValidationInvalid() {
    OptionsParser.parse(
        ImmutableMap.<String, String>builder().put("foo", "x").put("bar", "-1").build(),
        TestType3.class);
  }

  public static class TestType1 implements Options {
    public String foo;
    public int bar;
  }

  public static class TestType2 implements Options {
    @JsonProperty(value = "test.foo")
    public String foo;

    @JsonProperty(value = "test.bar")
    public int bar;
  }

  public static class TestType3 implements Options {
    @NotNull public String foo;
    @Positive public int bar;
  }
}
