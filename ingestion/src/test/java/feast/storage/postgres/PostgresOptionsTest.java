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

package feast.storage.postgres;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import feast.options.OptionsParser;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@Slf4j
public class PostgresOptionsTest {
  @Test
  public void testJdbcOptionsUrl() {
    PostgresOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("postgres.url", "jdbc:postgresql://host:port/database")
                .build(),
            PostgresOptions.class);
    assertEquals("jdbc:postgresql://host:port/database", options.url);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJdbcOptionsUrlInvalid() {
    PostgresOptions options =
        OptionsParser.parse(
            ImmutableMap.<String, String>builder()
                .put("postgres.url", "jdbc:missingpostgresprefix://fadfas")
                .build(),
            PostgresOptions.class);
  }

  @Test
  public void testJdbcOptionsPrefix() {
    assertEquals(
        "abcd",
        OptionsParser.parse(
                ImmutableMap.<String, String>builder()
                    .put("postgres.url", "jdbc:postgresql://host:port/database")
                    .put("postgres.prefix", "abcd")
                    .build(),
                PostgresOptions.class)
            .prefix);

    assertEquals(
        "ABCD",
        OptionsParser.parse(
                ImmutableMap.<String, String>builder()
                    .put("postgres.url", "jdbc:postgresql://host:port/database")
                    .put("postgres.prefix", "ABCD")
                    .build(),
                PostgresOptions.class)
            .prefix);

    assertEquals(
        "_abc",
        OptionsParser.parse(
                ImmutableMap.<String, String>builder()
                    .put("postgres.url", "jdbc:postgresql://host:port/database")
                    .put("postgres.prefix", "_abc")
                    .build(),
                PostgresOptions.class)
            .prefix);

    assertEquals(
        "abcd_ABCD_1234",
        OptionsParser.parse(
                ImmutableMap.<String, String>builder()
                    .put("postgres.url", "jdbc:postgresql://host:port/database")
                    .put("postgres.prefix", "abcd_ABCD_1234")
                    .build(),
                PostgresOptions.class)
            .prefix);
  }

  /**
   * Test that invalid prefix is rejected
   */
  @Test
  public void testJdbcOptionsPrefixInvalid() {
    try {
      OptionsParser.parse(
          ImmutableMap.<String, String>builder()
              .put("postgres.url", "jdbc:postgresql://host:port/database")
              .put("postgres.prefix", "9123abcd")
              .build(),
          PostgresOptions.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("property \"prefix\" must match"));
    }

    try {
      OptionsParser.parse(
          ImmutableMap.<String, String>builder()
              .put("postgres.url", "jdbc:postgresql://host:port/database")
              .put("postgres.prefix", "-asdf")
              .build(),
          PostgresOptions.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("property \"prefix\" must match"));
    }

    try {
      OptionsParser.parse(
          ImmutableMap.<String, String>builder()
              .put("postgres.url", "jdbc:postgresql://host:port/database")
              .put("postgres.prefix", "$asdf")
              .build(),
          PostgresOptions.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("property \"prefix\" must match"));
    }
  }
}
