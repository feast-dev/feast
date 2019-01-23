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
package feast.storage.bigtable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import feast.ingestion.model.Specs;
import feast.types.FeatureProto.Feature;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FeatureRowToBigTableMutationDoFnTest {
  @Rule public TestPipeline testPipeline = TestPipeline.create();
  FeatureRowToBigTableMutationDoFn doFn;
  private Specs specs;

  @Before
  public void setUp() {
    specs = mock(Specs.class, withSettings().serializable());
    doFn = new FeatureRowToBigTableMutationDoFn("table_prefix", specs);
  }

  @Test
  public void shouldIgnoreUnknownFeatureId() {
    FeatureRow row =
        FeatureRow.newBuilder()
            .setEntityKey("1234")
            .setEntityName("testEntity")
            // this feature should be ignored
            .addFeatures(Feature.newBuilder().setId("testEntity.none.unknown_feature"))
            .build();
    FeatureRowExtended rowExtended = FeatureRowExtended.newBuilder().setRow(row).build();
    PCollection<FeatureRowExtended> p = testPipeline.apply(Create.of(rowExtended));
    PCollection<KV<String, Mutation>> out = p.apply(ParDo.of(doFn));
    PAssert.that(out).empty();

    testPipeline.run();
  }
}
