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
package feast.ingestion.transform.fn;

import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.ValueProto.Value;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FilterFeatureRowDoFnTest {
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void shouldIgnoreUnspecifiedFeatureID() {
    String featureId1 = "testentity.none.feature1";
    String featureId2 = "testentity.hour.feature2";
    String featureId3 = "testentity.day.feature3";

    List<String> specifiedFeatureIds = Arrays.asList(featureId1, featureId2, featureId3);
    FilterFeatureRowDoFn doFn = new FilterFeatureRowDoFn(specifiedFeatureIds);

    FeatureRow row =
        FeatureRow.newBuilder()
            .setEntityKey("1234")
            .setEntityName("testentity")
            .addFeatures(
                Feature.newBuilder().setId(featureId1).setValue(Value.newBuilder().setInt64Val(10)))
            .addFeatures(
                Feature.newBuilder().setId(featureId2).setValue(Value.newBuilder().setInt64Val(11)))
            .addFeatures(
                Feature.newBuilder().setId(featureId3).setValue(Value.newBuilder().setInt64Val(12)))
            // this feature should be ignored
            .addFeatures(Feature.newBuilder().setId("testEntity.none.unknown_feature"))
            .build();

    PCollection<FeatureRow> output = testPipeline.apply(Create.of(row))
        .apply(ParDo.of(doFn));

    FeatureRow expRow =
        FeatureRow.newBuilder()
            .setEntityKey("1234")
            .setEntityName("testentity")
            .addFeatures(
                Feature.newBuilder().setId(featureId1).setValue(Value.newBuilder().setInt64Val(10)))
            .addFeatures(
                Feature.newBuilder().setId(featureId2).setValue(Value.newBuilder().setInt64Val(11)))
            .addFeatures(
                Feature.newBuilder().setId(featureId3).setValue(Value.newBuilder().setInt64Val(12)))
            .build();
    PAssert.that(output).containsInAnyOrder(expRow);

    testPipeline.run();
  }
}
