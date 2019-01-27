/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.ingestion.transform;

import static junit.framework.TestCase.assertNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import feast.ingestion.model.Features;
import feast.ingestion.model.Specs;
import feast.ingestion.model.Values;
import feast.ingestion.service.MockSpecService;
import feast.ingestion.transform.FeatureIO.Write;
import feast.ingestion.transform.SplitOutputByStore.WriteTags;
import feast.ingestion.values.PFeatureRows;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.FeatureStoreFactory;
import feast.storage.MockFeatureStore;
import feast.storage.MockTransforms;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class SplitOutputByStoreTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSplit() {
    // Note we are stores on the group, instead of warehouse or serving store id.
    SerializableFunction<FeatureSpec, String> selector = (featureSpec) -> featureSpec
        .getDataStores().getServing().getId();
    MockSpecService specService = new MockSpecService();
    specService
        .addEntity(EntitySpec.newBuilder().setName("e1").build())
        .addFeature(FeatureSpec.newBuilder().setId("f1").setEntity("e1").setGroup("store1")
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId("store1"))).build())
        .addFeature(FeatureSpec.newBuilder().setId("f2").setEntity("e1").setGroup("store2")
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId("store2"))).build())
        .addStorage(StorageSpec.newBuilder().setId("store1").setType("type1").build())
        .addStorage(StorageSpec.newBuilder().setId("store2").setType("type2").build());
    List<FeatureStoreFactory> stores =
        Lists.newArrayList(new MockFeatureStore("type1"), new MockFeatureStore("type2"));
    Specs specs =
        Specs.of(
            "jobname",
            ImportSpec.newBuilder()
                .addEntities("e1")
                .setSchema(
                    Schema.newBuilder()
                        .addAllFields(
                            Lists.newArrayList(
                                Field.newBuilder().setFeatureId("f1").build(),
                                Field.newBuilder().setFeatureId("f2").build())))
                .build(),
            specService);
    assertNull(specs.getError());

    SplitOutputByStore split = new SplitOutputByStore(stores, selector, specs);

    PCollection<FeatureRowExtended> input =
        pipeline
            .apply(
                Create.of(
                    FeatureRow.newBuilder()
                        .addFeatures(Features.of("f1", Values.ofInt32(1)))
                        .addFeatures(Features.of("f2", Values.ofInt32(2)))
                        .build()))
            .apply(new ToFeatureRowExtended());
    PFeatureRows pfrows = PFeatureRows.of(input);
    pfrows = pfrows.apply("do split", split);

    PAssert.that(
        pfrows
            .getErrors()).empty();
    PAssert.that(
        pfrows
            .getMain()
            .apply(
                MapElements.into(TypeDescriptor.of(FeatureRow.class))
                    .via(FeatureRowExtended::getRow)))
        .containsInAnyOrder(
            Lists.newArrayList(
                FeatureRow.newBuilder()
                    .addFeatures(Features.of("f1", Values.ofInt32(1)))
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .build(),
                FeatureRow.newBuilder()
                    .addFeatures(Features.of("f2", Values.ofInt32(2)))
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .build()));

    MockTransforms.Write mockSpecService1 = ((MockFeatureStore) stores.get(0)).getWrite();
    MockTransforms.Write mockSpecService2 = ((MockFeatureStore) stores.get(1)).getWrite();

    PCollection<FeatureRow> store1Output =
        mockSpecService1
            .getInputs()
            .get(0)
            .apply("map store1 outputs",
                MapElements.into(TypeDescriptor.of(FeatureRow.class))
                    .via(FeatureRowExtended::getRow));

    PCollection<FeatureRow> store2Output =
        mockSpecService2
            .getInputs()
            .get(0)
            .apply("map store2 outputs",
                MapElements.into(TypeDescriptor.of(FeatureRow.class))
                    .via(FeatureRowExtended::getRow));

    PAssert.that(store1Output)
        .containsInAnyOrder(
            Lists.newArrayList(
                FeatureRow.newBuilder()
                    .addFeatures(Features.of("f1", Values.ofInt32(1)))
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .build()));

    PAssert.that(store2Output)
        .containsInAnyOrder(
            Lists.newArrayList(
                FeatureRow.newBuilder()
                    .addFeatures(Features.of("f2", Values.ofInt32(2)))
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .build()));
    pipeline.run();
  }

  @Test
  public void testSplitWhereFeature2HasNoStoreId() {
    // Feature2 should get thrown away harmlessly

    SerializableFunction<FeatureSpec, String> selector = (fs) -> fs.getDataStores().getServing()
        .getId();
    MockSpecService specService = new MockSpecService();
    specService.entitySpecs.put("e1", EntitySpec.getDefaultInstance());
    specService.featureSpecs.put(
        "f1", FeatureSpec.newBuilder().setEntity("e1").setDataStores(
            DataStores.newBuilder().setServing(DataStore.newBuilder().setId("store1")))
            .build());

    // f2 doesn't set a serving store, this is considered okay, this feature is ignored without error.
    specService.featureSpecs.put(
        "f2", FeatureSpec.newBuilder().setEntity("e1").build());
    specService.storageSpecs.put(
        "store1", StorageSpec.newBuilder().setId("store1").setType("type1").build());
    specService.storageSpecs.put(
        "store2", StorageSpec.newBuilder().setId("store2").setType("type2").build());
    List<FeatureStoreFactory> stores =
        Lists.newArrayList(new MockFeatureStore("type1"), new MockFeatureStore("type2"));
    Specs specs =
        Specs.of(
            "jobname",
            ImportSpec.newBuilder()
                .addEntities("e1")
                .setSchema(
                    Schema.newBuilder()
                        .addAllFields(
                            Lists.newArrayList(
                                Field.newBuilder().setFeatureId("f1").build(),
                                Field.newBuilder().setFeatureId("f2").build())))
                .build(),
            specService);
    assertNull(specs.getError());

    SplitOutputByStore split = new SplitOutputByStore(stores, selector, specs);

    PCollection<FeatureRowExtended> input =
        pipeline
            .apply(
                Create.of(
                    FeatureRow.newBuilder()
                        .addFeatures(Features.of("f1", Values.ofInt32(1)))
                        .addFeatures(Features.of("f2", Values.ofInt32(2)))
                        .build()))
            .apply(new ToFeatureRowExtended());
    PFeatureRows pfrows = PFeatureRows.of(input);
    pfrows = pfrows.apply("do split", split);

    PAssert.that(
        pfrows
            .getErrors()).empty();
    PAssert.that(
        pfrows
            .getMain()
            .apply(
                MapElements.into(TypeDescriptor.of(FeatureRow.class))
                    .via(FeatureRowExtended::getRow)))
        .containsInAnyOrder(
            FeatureRow.newBuilder()
                .addFeatures(Features.of("f1", Values.ofInt32(1)))
                .setEventTimestamp(Timestamp.getDefaultInstance())
                .build(),
            FeatureRow.newBuilder()
                .addFeatures(Features.of("f2", Values.ofInt32(2)))
                .setEventTimestamp(Timestamp.getDefaultInstance())
                .build());

    MockTransforms.Write mockSpecService1 = ((MockFeatureStore) stores.get(0)).getWrite();

    PCollection<FeatureRow> store1Output =
        mockSpecService1
            .getInputs()
            .get(0)
            .apply("map store1 outputs",
                MapElements.into(TypeDescriptor.of(FeatureRow.class))
                    .via(FeatureRowExtended::getRow));

    PAssert.that(store1Output)
        .containsInAnyOrder(
            Lists.newArrayList(
                FeatureRow.newBuilder()
                    .addFeatures(Features.of("f1", Values.ofInt32(1)))
                    .setEventTimestamp(Timestamp.getDefaultInstance())
                    .build()));

    pipeline.run();
  }


  @Test
  public void testSplitWhereNoStorageSpec() {
    // There is no storage spec registered and feature 1 has no storage set
    // feature1 should get thrown harmlessly

    SerializableFunction<FeatureSpec, String> selector = (fs) -> fs.getDataStores().getServing()
        .getId();
    MockSpecService specService = new MockSpecService();
    specService.entitySpecs.put("e1", EntitySpec.getDefaultInstance());
    specService.featureSpecs.put(
        "f1", FeatureSpec.newBuilder().setEntity("e1")
            .build());

    Specs specs =
        Specs.of(
            "jobname",
            ImportSpec.newBuilder()
                .addEntities("e1")
                .setSchema(
                    Schema.newBuilder()
                        .addAllFields(
                            Collections.singletonList(
                                Field.newBuilder().setFeatureId("f1").build())))
                .build(),
            specService);
    assertNull(specs.getError());
    List<FeatureStoreFactory> stores = Collections.emptyList();
    SplitOutputByStore split = new SplitOutputByStore(stores, selector, specs);

    PCollection<FeatureRowExtended> input =
        pipeline
            .apply(
                Create.of(
                    FeatureRow.newBuilder()
                        .addFeatures(Features.of("f1", Values.ofInt32(1)))
                        .build()))
            .apply(new ToFeatureRowExtended());
    PFeatureRows pfrows = PFeatureRows.of(input);
    pfrows = pfrows.apply("do split", split);

    PAssert.that(
        pfrows
            .getErrors()).empty();
    PAssert.that(
        pfrows
            .getMain()
            .apply(
                MapElements.into(TypeDescriptor.of(FeatureRow.class))
                    .via(FeatureRowExtended::getRow)))
        .containsInAnyOrder(
            FeatureRow.newBuilder()
                .addFeatures(Features.of("f1", Values.ofInt32(1)))
                .setEventTimestamp(Timestamp.getDefaultInstance())
                .build());

    pipeline.run();
  }


  @Test
  public void testSplitWhereNoStorageSpecForAFeature() {
    // There is no storage spec registered but feature 1 has storage set
    // feature1 should get thrown harmlessly

    SerializableFunction<FeatureSpec, String> selector = (fs) -> fs.getDataStores().getServing()
        .getId();
    MockSpecService specService = new MockSpecService();
    specService.entitySpecs.put("e1", EntitySpec.getDefaultInstance());
    specService.featureSpecs.put(
        "f1", FeatureSpec.newBuilder().setEntity("e1")
            .setDataStores(
                DataStores.newBuilder().setServing(DataStore.newBuilder().setId("store1")
                    .build()))
            .build());

    Specs specs =
        Specs.of(
            "jobname",
            ImportSpec.newBuilder()
                .addEntities("e1")
                .setSchema(
                    Schema.newBuilder()
                        .addAllFields(
                            Collections.singletonList(
                                Field.newBuilder().setFeatureId("f1").build())))
                .build(),
            specService);
    assertNull(specs.getError());
    List<FeatureStoreFactory> stores = Collections.emptyList();
    SplitOutputByStore split = new SplitOutputByStore(stores, selector, specs);

    PCollection<FeatureRowExtended> input =
        pipeline
            .apply(
                Create.of(
                    FeatureRow.newBuilder()
                        .addFeatures(Features.of("f1", Values.ofInt32(1)))
                        .build()))
            .apply(new ToFeatureRowExtended());
    PFeatureRows pfrows = PFeatureRows.of(input);
    pfrows = pfrows.apply("do split", split);

    PAssert.that(
        pfrows
            .getErrors()).empty();
    PAssert.that(
        pfrows
            .getMain()
            .apply(
                MapElements.into(TypeDescriptor.of(FeatureRow.class))
                    .via(FeatureRowExtended::getRow)))
        .containsInAnyOrder(
            FeatureRow.newBuilder()
                .addFeatures(Features.of("f1", Values.ofInt32(1)))
                .setEventTimestamp(Timestamp.getDefaultInstance())
                .build());

    pipeline.run();
  }

  @Test
  public void testWriteTags() {
    TupleTag<FeatureRowExtended> tag1 = new TupleTag<>("TAG1");
    TupleTag<FeatureRowExtended> tag2 = new TupleTag<>("TAG2");
    TupleTag<FeatureRowExtended> tag3 = new TupleTag<>("TAG3");
    TupleTag<FeatureRowExtended> mainTag = new TupleTag<>("TAG4");

    Map<TupleTag<FeatureRowExtended>, Write> transforms = ImmutableMap.<TupleTag<FeatureRowExtended>, Write>builder()
        .put(tag1, new MockTransforms.Write())
        .put(tag2, new MockTransforms.Write())
        // tag3 and mainTag do not have write transforms.
        .build();

    FeatureRowExtended rowex1 = FeatureRowExtended.newBuilder()
        .setRow(FeatureRow.newBuilder().setEntityKey("1")).build();
    FeatureRowExtended rowex2 = FeatureRowExtended.newBuilder()
        .setRow(FeatureRow.newBuilder().setEntityKey("2")).build();
    FeatureRowExtended rowex3 = FeatureRowExtended.newBuilder()
        .setRow(FeatureRow.newBuilder().setEntityKey("3")).build();
    FeatureRowExtended rowex4 = FeatureRowExtended.newBuilder()
        .setRow(FeatureRow.newBuilder().setEntityKey("4")).build();

    PCollection<FeatureRowExtended> pcollection1 = pipeline.apply("input1", Create.of(rowex1));
    PCollection<FeatureRowExtended> pcollection2 = pipeline.apply("input2", Create.of(rowex2));
    PCollection<FeatureRowExtended> pcollection3 = pipeline.apply("input3", Create.of(rowex3));
    PCollection<FeatureRowExtended> pcollection4 = pipeline.apply("input4", Create.of(rowex4));

    PCollectionTuple tuple = PCollectionTuple.of(tag1, pcollection1).and(tag2, pcollection2)
        .and(tag3, pcollection3)
        .and(mainTag, pcollection4); // input 3 is included in the output

    PCollection<FeatureRowExtended> output = tuple.apply(new WriteTags(transforms, mainTag));

    // All input should be in the main output.
    // input 1 is returned in the output because we wrote it to a write transform.
    // input 2 is returned in the output because we wrote it to a write transform.
    // input 3 is NOT returned in the output because it has no write transform.
    // input 4 is is returned in the output because it is the main tag.
    PAssert.that(output).containsInAnyOrder(rowex1, rowex2, rowex4);

    // Each non main tagged input should be written to corresponding Write transform
    PAssert.that(((MockTransforms.Write) transforms.get(tag1)).getInputs().get(0))
        .containsInAnyOrder(rowex1);
    PAssert.that(((MockTransforms.Write) transforms.get(tag2)).getInputs().get(0))
        .containsInAnyOrder(rowex2);
    pipeline.run();
  }
}
