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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.protobuf.Timestamp;
import feast.ingestion.model.Specs;
import feast.ingestion.service.FileSpecService;
import feast.ingestion.service.SpecService;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity.Enum;
import feast.types.ValueProto.Value;
import java.util.Collections;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ConvertTypesDoFnTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();
  private ConvertTypesDoFn doFn;
  private Specs specs;
  private SpecService fileSpecService;

  @Before
  public void setUp() {
    fileSpecService =
        new FileSpecService(getClass().getClassLoader().getResource("core_specs").getPath());
    specs = mock(Specs.class, withSettings().serializable());
    doFn = new ConvertTypesDoFn(specs);
  }

  @Test
  public void shouldIgnoreUnknownFeatureId() {
    when(specs.tryGetFeatureSpec("testEntity.day.testInt64"))
        .thenReturn(
            fileSpecService
                .getFeatureSpecs(Collections.singletonList("testEntity.day.testInt64"))
                .get("testEntity.day.testInt64"));

    FeatureRow row =
        FeatureRow.newBuilder()
            .setEntityKey("1234")
            .setEntityName("testEntity")
            .addFeatures(
                Feature.newBuilder()
                    .setId("testEntity.day.testInt64")
                    .setValue(Value.newBuilder().setInt64Val(10)))
            // this feature should be ignored
            .addFeatures(Feature.newBuilder().setId("testEntity.none.unknown_feature"))
            .build();
    FeatureRowExtended rowExtended = FeatureRowExtended.newBuilder().setRow(row).build();
    PCollection<FeatureRowExtended> p = testPipeline.apply(Create.of(rowExtended));
    PCollection<FeatureRowExtended> out = p.apply(ParDo.of(doFn));

    FeatureRow expRow =
        FeatureRow.newBuilder()
            .setEntityKey("1234")
            .setEntityName("testEntity")
            .addFeatures(
                Feature.newBuilder()
                    .setId("testEntity.day.testInt64")
                    .setValue(Value.newBuilder().setInt64Val(10)))
            .setGranularity(Enum.DAY)
            .setEventTimestamp(Timestamp.getDefaultInstance())
            .build();

    FeatureRowExtended expected =
        FeatureRowExtended.newBuilder()
            .setRow(expRow)
            .setLastAttempt(Attempt.getDefaultInstance())
            .build();
    PAssert.that(out).containsInAnyOrder(expected);

    testPipeline.run();
  }
}
