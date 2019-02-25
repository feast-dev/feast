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

package feast.ingestion.transform.fn;


import static feast.FeastMatchers.hasCount;

import com.google.common.collect.Lists;
import feast.ingestion.model.Features;
import feast.ingestion.model.Specs;
import feast.ingestion.model.Values;
import feast.ingestion.util.DateUtil;
import feast.ingestion.values.PFeatureRows;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.ValueType.Enum;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

@Slf4j
public class ConvertTypesDoFnTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testStringTo() {
    FeatureRowExtended row = FeatureRowExtended.newBuilder().setRow(
        FeatureRow.newBuilder().addAllFeatures(Lists.newArrayList(
            Features.of("STRING_TO_INT32", Values.ofString("123")),
            Features.of("STRING_TO_INT64", Values.ofString("123")),
            Features.of("STRING_TO_FLOAT", Values.ofString("123")),
            Features.of("STRING_TO_DOUBLE", Values.ofString("123")),
            Features.of("STRING_TO_STRING", Values.ofString("123")),
            Features.of("STRING_TO_BOOL", Values.ofString("true")),
            Features.of("STRING_TO_TIMESTAMP", Values.ofString("2019-01-31T19:19:19.123Z"))
        ))).build();

    Map<String, FeatureSpec> featureSpecs = new HashMap<>();
    featureSpecs.put("STRING_TO_INT32", FeatureSpec.newBuilder().setValueType(Enum.INT32).build());
    featureSpecs.put("STRING_TO_INT64", FeatureSpec.newBuilder().setValueType(Enum.INT64).build());
    featureSpecs.put("STRING_TO_FLOAT", FeatureSpec.newBuilder().setValueType(Enum.FLOAT).build());
    featureSpecs.put("STRING_TO_DOUBLE", FeatureSpec.newBuilder().setValueType(Enum.DOUBLE).build());
    featureSpecs.put("STRING_TO_BOOL", FeatureSpec.newBuilder().setValueType(Enum.BOOL).build());
    featureSpecs.put("STRING_TO_STRING", FeatureSpec.newBuilder().setValueType(Enum.STRING).build());
    featureSpecs.put("STRING_TO_TIMESTAMP", FeatureSpec.newBuilder().setValueType(Enum.TIMESTAMP).build());

    PFeatureRows output = PFeatureRows.of(pipeline.apply(Create.of(row)))
        .applyDoFn("name",
            new ConvertTypesDoFn(Specs.builder().featureSpecs(featureSpecs).build()));


    PAssert.that(output.getErrors()).satisfies(rows -> {
      if (rows.iterator().hasNext()) {
        log.error(rows.iterator().next().getLastAttempt().getError().toString());
      }
      return null;
    });

    PAssert.that(output.getMain()).containsInAnyOrder(FeatureRowExtended.newBuilder().setRow(
        FeatureRow.newBuilder().addAllFeatures(Lists.newArrayList(
            Features.of("STRING_TO_INT32", Values.ofInt32(123)),
            Features.of("STRING_TO_INT64", Values.ofInt64(123L)),
            Features.of("STRING_TO_FLOAT", Values.ofFloat(123F)),
            Features.of("STRING_TO_DOUBLE", Values.ofDouble(123.0)),
            Features.of("STRING_TO_STRING", Values.ofString("123")),
            Features.of("STRING_TO_BOOL", Values.ofBool(true)),
            Features.of("STRING_TO_TIMESTAMP",
                Values.ofTimestamp(DateUtil.toTimestamp("2019-01-31T19:19:19.123Z")))
        ))).setLastAttempt(Attempt.getDefaultInstance()).build());
    pipeline.run();
  }
}