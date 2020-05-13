/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
 */
package feast.ingestion.transform;

import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.storage.api.writer.FailedElement;
import feast.test.TestUtil;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;

public class ProcessAndValidateFeatureRowsTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static final TupleTag<FeatureRow> SUCCESS_TAG = new TupleTag<FeatureRow>() {};

  private static final TupleTag<FailedElement> FAILURE_TAG = new TupleTag<FailedElement>() {};

  @Test
  public void shouldWriteSuccessAndFailureTagsCorrectly() {
    FeatureSetSpec fs1 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_1").setValueType(Enum.STRING).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_2").setValueType(Enum.INT64).build())
            .build();

    FeatureSetSpec fs2 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set_2")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_1").setValueType(Enum.STRING).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_2").setValueType(Enum.INT64).build())
            .build();

    Map<String, FeatureSetSpec> featureSetSpecs = new HashMap<>();
    featureSetSpecs.put("myproject/feature_set", fs1);
    featureSetSpecs.put("myproject/feature_set_2", fs2);

    List<FeatureRow> input = new ArrayList<>();
    List<FeatureRow> expected = new ArrayList<>();

    for (FeatureSetSpec featureSetSpec : featureSetSpecs.values()) {
      FeatureRow randomRow = TestUtil.createRandomFeatureRow(featureSetSpec);
      input.add(randomRow);
      expected.add(randomRow);
    }

    input.add(FeatureRow.newBuilder().setFeatureSet("invalid").build());

    PCollectionTuple output =
        p.apply(Create.of(input))
            .setCoder(ProtoCoder.of(FeatureRow.class))
            .apply(
                ProcessAndValidateFeatureRows.newBuilder()
                    .setFailureTag(FAILURE_TAG)
                    .setSuccessTag(SUCCESS_TAG)
                    .setFeatureSetSpecs(featureSetSpecs)
                    .build());

    PAssert.that(output.get(SUCCESS_TAG)).containsInAnyOrder(expected);
    PAssert.that(output.get(FAILURE_TAG).apply(Count.globally())).containsInAnyOrder(1L);

    p.run();
  }

  @Test
  public void shouldStripVersions() {
    FeatureSetSpec fs1 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_1").setValueType(Enum.STRING).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_2").setValueType(Enum.INT64).build())
            .build();

    Map<String, FeatureSetSpec> featureSetSpecs = new HashMap<>();
    featureSetSpecs.put("myproject/feature_set", fs1);

    List<FeatureRow> input = new ArrayList<>();
    List<FeatureRow> expected = new ArrayList<>();

    FeatureRow randomRow = TestUtil.createRandomFeatureRow(fs1);
    expected.add(randomRow);
    randomRow = randomRow.toBuilder().setFeatureSet("myproject/feature_set:1").build();
    input.add(randomRow);

    PCollectionTuple output =
        p.apply(Create.of(input))
            .setCoder(ProtoCoder.of(FeatureRow.class))
            .apply(
                ProcessAndValidateFeatureRows.newBuilder()
                    .setFailureTag(FAILURE_TAG)
                    .setSuccessTag(SUCCESS_TAG)
                    .setFeatureSetSpecs(featureSetSpecs)
                    .build());

    PAssert.that(output.get(SUCCESS_TAG)).containsInAnyOrder(expected);

    p.run();
  }

  @Test
  public void shouldExcludeUnregisteredFields() {
    FeatureSetSpec fs1 =
        FeatureSetSpec.newBuilder()
            .setName("feature_set")
            .setProject("myproject")
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_primary")
                    .setValueType(Enum.INT32)
                    .build())
            .addEntities(
                EntitySpec.newBuilder()
                    .setName("entity_id_secondary")
                    .setValueType(Enum.STRING)
                    .build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_1").setValueType(Enum.STRING).build())
            .addFeatures(
                FeatureSpec.newBuilder().setName("feature_2").setValueType(Enum.INT64).build())
            .build();

    Map<String, FeatureSetSpec> featureSets = new HashMap<>();
    featureSets.put("myproject/feature_set", fs1);

    List<FeatureRow> input = new ArrayList<>();
    List<FeatureRow> expected = new ArrayList<>();

    FeatureRow randomRow = TestUtil.createRandomFeatureRow(fs1);
    expected.add(randomRow);
    input.add(
        randomRow
            .toBuilder()
            .addFields(
                Field.newBuilder()
                    .setName("extra")
                    .setValue(Value.newBuilder().setStringVal("hello")))
            .build());

    PCollectionTuple output =
        p.apply(Create.of(input))
            .setCoder(ProtoCoder.of(FeatureRow.class))
            .apply(
                ProcessAndValidateFeatureRows.newBuilder()
                    .setFailureTag(FAILURE_TAG)
                    .setSuccessTag(SUCCESS_TAG)
                    .setFeatureSetSpecs(featureSets)
                    .build());

    PAssert.that(output.get(SUCCESS_TAG)).containsInAnyOrder(expected);

    p.run();
  }
}
