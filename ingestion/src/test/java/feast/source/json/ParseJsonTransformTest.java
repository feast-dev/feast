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

package feast.source.json;

import static org.junit.Assert.assertEquals;

import feast.ingestion.model.Values;
import feast.source.json.ParseJsonTransform;
import feast.types.ValueProto.Value;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

@Slf4j
public class ParseJsonTransformTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testJsonBool() {
    PCollection<Map<String, Value>> output = pipeline.apply(Create.of("{\"x\": true}"))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals(true, map.get("x").getBoolVal());
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testJsonString() {
    PCollection<Map<String, Value>> output = pipeline.apply(Create.of("{\"x\": \"abc\"}"))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals("abc", map.get("x").getStringVal());
      return null;
    });
    pipeline.run();
  }


  @Test
  public void testJsonObject() {
    PCollection<Map<String, Value>> output = pipeline.apply(Create.of("{\"x\": {}}"))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals("{}", map.get("x").getStringVal());
      return null;
    });
    pipeline.run();
  }


  @Test
  public void testJsonLong() {
    PCollection<Map<String, Value>> output = pipeline
        .apply(Create
            .of(String.format("{\"max\": %s, \"min\": %s}", Long.MAX_VALUE, Long.MIN_VALUE)))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals(Values.ofInt64(Long.MAX_VALUE), map.get("max"));
      assertEquals(Values.ofInt64(Long.MIN_VALUE), map.get("min"));
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testJsonInteger() {
    PCollection<Map<String, Value>> output = pipeline
        .apply(Create
            .of(String.format("{\"max\": %s, \"min\": %s}", Integer.MAX_VALUE, Integer.MIN_VALUE)))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals(Values.ofInt64(Integer.MAX_VALUE), map.get("max"));
      assertEquals(Values.ofInt64(Integer.MIN_VALUE), map.get("min"));
      return null;
    });
    pipeline.run();
  }


  @Test
  public void testJsonDouble() {
    PCollection<Map<String, Value>> output = pipeline
        .apply(Create
            .of(String.format("{\"max\": %s, \"min\": %s}", Double.MAX_VALUE, Double.MIN_VALUE)))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals(Values.ofDouble(Double.MAX_VALUE), map.get("max"));
      assertEquals(Values.ofDouble(Double.MIN_VALUE), map.get("min"));
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testJsonFloat() {
    PCollection<Map<String, Value>> output = pipeline
        .apply(Create
            .of(String
                .format("{\"max\": %s, \"min\": %s, \"x\": %s}", Float.MAX_VALUE, Float.MIN_VALUE,
                    0.12345678f)))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals(Values.ofDouble(Float.MAX_VALUE).getDoubleVal(),
          map.get("max").getDoubleVal(), Float.MAX_VALUE / 2E7);
      assertEquals(Values.ofDouble(0.12345678F).getDoubleVal(), map.get("x").getDoubleVal(),
          0.00000001);
      assertEquals(Values.ofDouble(Float.MIN_VALUE).getDoubleVal(),
          map.get("min").getDoubleVal(), Float.MIN_VALUE * 2E8);

      return null;
    });
    pipeline.run();
  }

  @Test
  public void testJsonArray() {
    PCollection<Map<String, Value>> output = pipeline.apply(Create.of("{\"x\": []}"))
        .apply(new ParseJsonTransform());
    PAssert.that(output).satisfies((maps) -> {
      Map<String, Value> map = maps.iterator().next();
      assertEquals("[]", map.get("x").getStringVal());
      return null;
    });
    pipeline.run();
  }
}
