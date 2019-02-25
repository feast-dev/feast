/*
 * Copyright 2019 The Feast Authors
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

package feast.source.csv;

import static feast.FeastMatchers.hasCount;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import feast.source.csv.ParseCsvTransform.CSVLineParser;
import feast.source.csv.ParseCsvTransform.StringMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

@Slf4j
public class ParseCSVTransformTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEmptyLine() {
    PCollection<Map<String, String>> output = pipeline.apply(Create.of(""))
        .apply(ParseCsvTransform.builder().header(Lists.newArrayList()).build());
    PAssert.that(output).satisfies(hasCount(0));
    pipeline.run();
  }

  @Test
  public void testAllEmptyFields() {
    PCollection<Map<String, String>> output = pipeline.apply(Create.of(",,"))
        .apply(ParseCsvTransform.builder().header(Lists.newArrayList("a", "b", "c")).build());
    PAssert.that(output).satisfies(hasCount(1));
    PAssert.that(output).satisfies(rows -> {
      Map<String, String> row = rows.iterator().next();
      assertEquals(row.size(), 3);
      assertEquals(row.get("a"), "");
      assertEquals(row.get("b"), "");
      assertEquals(row.get("c"), "");
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testSomeEmptyFields() {
    PCollection<Map<String, String>> output = pipeline.apply(Create.of("1,,3"))
        .apply(ParseCsvTransform.builder().header(Lists.newArrayList("a", "b", "c")).build());
    PAssert.that(output).satisfies(hasCount(1));
    PAssert.that(output).satisfies(rows -> {
      Map<String, String> row = rows.iterator().next();
      assertEquals(row.size(), 3);
      assertEquals(row.get("a"), "1");
      assertEquals(row.get("b"), "");
      assertEquals(row.get("c"), "3");
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testTooManyHeaders() {
    PCollection<Map<String, String>> output = pipeline.apply(Create.of("1,2"))
        .apply(ParseCsvTransform.builder().header(Lists.newArrayList("a", "b", "c")).build());
    PAssert.that(output).satisfies(hasCount(1));
    PAssert.that(output).satisfies(rows -> {
      Map<String, String> row = rows.iterator().next();
      assertEquals(row.size(), 2);
      assertEquals(row.get("a"), "1");
      assertEquals(row.get("b"), "2");
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testNotEnoughHeaders() {
    PCollection<Map<String, String>> output = pipeline.apply(Create.of("1,2,3"))
        .apply(ParseCsvTransform.builder().header(Lists.newArrayList("a", "b")).build());
    PAssert.that(output).satisfies(hasCount(1));
    PAssert.that(output).satisfies(rows -> {
      Map<String, String> row = rows.iterator().next();
      assertEquals(row.size(), 2);
      assertEquals(row.get("a"), "1");
      assertEquals(row.get("b"), "2");
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testCSVLineParser_multiLines() {
    CSVLineParser parser = new CSVLineParser(Lists.newArrayList("c1", "c2"));
    List<StringMap> actual = parser.records("a,b\nc,d");

    List<StringMap> expected = Lists.newArrayList(
        new StringMap().thisput("c1", "a").thisput("c2", "b"),
        new StringMap().thisput("c1", "c").thisput("c2", "d")
    );
    assertThat(actual, is(expected));
  }

  @Test
  public void testCSVLineParser_repeatedLines() {
    CSVLineParser parser = new CSVLineParser(Lists.newArrayList("c1", "c2"));
    List<StringMap> actual = parser.records("a,b");
    List<StringMap> expected = Lists.newArrayList(
        new StringMap().thisput("c1", "a").thisput("c2", "b")
    );
    assertThat(actual, is(expected));

    actual = parser.records("c,d");
    expected = Lists.newArrayList(
        new StringMap().thisput("c1", "c").thisput("c2", "d")
    );
    assertThat(actual, is(expected));
  }
}
