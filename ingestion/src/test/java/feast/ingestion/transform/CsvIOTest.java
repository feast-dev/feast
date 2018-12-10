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

package feast.ingestion.transform;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import com.google.common.collect.Lists;
import feast.ingestion.transform.CsvIO.CSVLineParser;
import feast.ingestion.transform.CsvIO.StringMap;
import java.util.List;
import junit.framework.TestCase;
import feast.ingestion.transform.CsvIO.CSVLineParser;
import feast.ingestion.transform.CsvIO.StringMap;

public class CsvIOTest extends TestCase {

  public void testCSVLineParser_multiLines() {
    CSVLineParser parser = new CSVLineParser(Lists.newArrayList("c1", "c2"));
    List<StringMap> actual = parser.records("a,b\nc,d");

    List<StringMap> expected = Lists.newArrayList(
        new StringMap().thisput("c1", "a").thisput("c2", "b"),
        new StringMap().thisput("c1", "c").thisput("c2", "d")
    );
    assertThat(actual, is(expected));
  }

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
