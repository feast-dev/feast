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

import static feast.source.common.StringToValueMapTransform.VALUE_MAP_CODER;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import feast.ingestion.model.Features;
import feast.ingestion.model.Values;
import feast.ingestion.util.DateUtil;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.Schema;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;

public class ValueMapToFeatureRowTransformTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();


  @Test
  public void testTimestampValue() {
    Schema schema = Schema.newBuilder()
        .setTimestampValue(DateUtil.toTimestamp(DateTime.now()))
        .build();
    Map<String, Value> map = new HashMap<>();

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals("entity", row.getEntityName());
      assertEquals(schema.getTimestampValue(), row.getEventTimestamp());
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testTimestampColumn() {
    Schema schema = Schema.newBuilder()
        .setTimestampColumn("timestamp")
        .addFields(Field.newBuilder().setName("c1").build()).build();

    Map<String, Value> map = new HashMap<>();
    map.put("timestamp", Values.asTimestamp(Values.ofString("2019-01-30T19:30:12Z")));

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals("entity", row.getEntityName());
      assertEquals(map.get("timestamp").getTimestampVal(), row.getEventTimestamp());
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testTimestampColumnOptional() {
    // schema field for timestamp column is optional, as long as it can be provided by the source
    Schema schema = Schema.newBuilder()
        .setTimestampColumn("timestamp").build();

    Map<String, Value> map = new HashMap<>();
    map.put("timestamp", Values.asTimestamp(Values.ofString("2019-01-30T19:30:12Z")));

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals("entity", row.getEntityName());
      assertEquals(map.get("timestamp").getTimestampVal(), row.getEventTimestamp());
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testEntityKeyColumn() {
    Schema schema = Schema.newBuilder()
        .setEntityIdColumn("driver_id").build();

    Map<String, Value> map = new HashMap<>();
    map.put("driver_id", Values.ofString("1234"));

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals("entity", row.getEntityName());
      assertEquals("1234", row.getEntityKey());
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testEntityKeyColumnTypeConverted() {
    Schema schema = Schema.newBuilder()
        .setEntityIdColumn("driver_id").build();

    Map<String, Value> map = new HashMap<>();
    map.put("driver_id", Values.ofInt64(12345)); //integer Value gets converted to string.

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals("entity", row.getEntityName());
      assertEquals("12345", row.getEntityKey());
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testFieldPickedWithFeatureId() {
    Schema schema = Schema.newBuilder()
        .addFields(Field.newBuilder().setName("x").setFeatureId("f1")).build();

    Map<String, Value> map = new HashMap<>();
    map.put("x", Values.ofInt64(1));

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals(1, row.getFeaturesList().size());
      assertEquals(Features.of("f1", Values.ofInt64(1)), row.getFeatures(0));

      return null;
    });
    pipeline.run();
  }

  @Test
  public void testFieldNonFeatureIdIgnored() {
    Schema schema = Schema.newBuilder()
        .addFields(Field.newBuilder().setName("x")).build();

    Map<String, Value> map = new HashMap<>();
    map.put("x", Values.ofInt64(1));

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals(0, row.getFeaturesList().size());
      return null;
    });
    pipeline.run();
  }

  @Test
  public void testFeatureWithValueNotSetIsIgnored() {
    Schema schema = Schema.newBuilder()
        .addFields(Field.newBuilder().setName("x").setFeatureId("f1")).build();

    Map<String, Value> map = new HashMap<>();
    map.put("x", Value.newBuilder().build());

    PCollection<FeatureRow> output = pipeline
        .apply(Create.of(Lists.newArrayList(map)).withCoder(VALUE_MAP_CODER))
        .apply(new ValueMapToFeatureRowTransform("entity", schema));

    PAssert.that(output).satisfies(maps -> {
      FeatureRow row = maps.iterator().next();
      assertEquals(0, row.getFeaturesList().size());
      return null;
    });
    pipeline.run();
  }
}