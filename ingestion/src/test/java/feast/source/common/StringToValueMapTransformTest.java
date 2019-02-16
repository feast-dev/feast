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

package feast.source.common;

import com.google.common.collect.Lists;
import feast.source.common.StringToValueMapTransform;
import feast.types.ValueProto.Value;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class StringToValueMapTransformTest {

  @Rule
  TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEmptyMap() {
    Map<String, String> map = new HashMap<>();
    PCollection<Map<String, String>> input = pipeline.apply(Create.of(Lists.newArrayList(map)));

    PCollection<Map<String, Value>> output = input.apply(new StringToValueMapTransform());

    PAssert.that(output).containsInAnyOrder(new HashMap<>());
  }

  @Test
  public void testEmptyValues() {
    Map<String, String> map = new HashMap<>();
    map.put("a", "");
    map.put("b", null);
    PCollection<Map<String, String>> input = pipeline.apply(Create.of(Lists.newArrayList(map)));

    PCollection<Map<String, Value>> output = input.apply(new StringToValueMapTransform());

    PAssert.that(output).containsInAnyOrder(new HashMap<>());
  }
}