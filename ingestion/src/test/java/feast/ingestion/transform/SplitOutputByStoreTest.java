package feast.ingestion.transform;

import static junit.framework.TestCase.assertNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Timestamp;
import feast.ingestion.model.Features;
import feast.ingestion.model.Specs;
import feast.ingestion.model.Values;
import feast.ingestion.service.SpecService;
import feast.ingestion.values.PFeatureRows;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.Field;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.ImportSpecProto.Schema;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.storage.FeatureStore;
import feast.storage.MockFeatureStore;
import feast.storage.MockTransforms;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

public class SplitOutputByStoreTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSplit() {
    // Note we are stores on the group, instead of warehouse or serving store id.
    SerializableFunction<FeatureSpec, String> selector = FeatureSpec::getGroup;
    MockSpecService specService = new MockSpecService();
    specService.entitySpecs.put("e1", EntitySpec.getDefaultInstance());
    specService.featureSpecs.put(
        "f1", FeatureSpec.newBuilder().setEntity("e1").setGroup("store1").build());
    specService.featureSpecs.put(
        "f2", FeatureSpec.newBuilder().setEntity("e1").setGroup("store2").build());
    specService.storageSpecs.put(
        "store1", StorageSpec.newBuilder().setId("store1").setType("type1").build());
    specService.storageSpecs.put(
        "store2", StorageSpec.newBuilder().setId("store2").setType("type2").build());
    List<FeatureStore> stores =
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
    // Note we are stores on the group, instead of warehouse or serving store id.
    SerializableFunction<FeatureSpec, String> selector = FeatureSpec::getGroup;
    MockSpecService specService = new MockSpecService();
    specService.entitySpecs.put("e1", EntitySpec.getDefaultInstance());
    specService.featureSpecs.put(
        "f1", FeatureSpec.newBuilder().setEntity("e1").setGroup("store1").build());
    specService.featureSpecs.put(
        "f2", FeatureSpec.newBuilder().setEntity("e1").build());
    specService.storageSpecs.put(
        "store1", StorageSpec.newBuilder().setId("store1").setType("type1").build());
    specService.storageSpecs.put(
        "store2", StorageSpec.newBuilder().setId("store2").setType("type2").build());
    List<FeatureStore> stores =
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

    pipeline.run();
  }

  public static class MockSpecService implements SpecService {

    public Map<String, EntitySpec> entitySpecs = new HashMap<>();
    public Map<String, FeatureSpec> featureSpecs = new HashMap<>();
    public Map<String, StorageSpec> storageSpecs = new HashMap<>();

    @Override
    public Map<String, EntitySpec> getEntitySpecs(Iterable<String> entityIds) {
      Set<String> entityIdsSet = Sets.newHashSet(entityIds);
      return Maps.newHashMap(
          Maps.filterEntries(entitySpecs, (entry) -> entityIdsSet.contains(entry.getKey())));
    }

    @Override
    public Map<String, EntitySpec> getAllEntitySpecs() {
      return entitySpecs;
    }

    @Override
    public Map<String, FeatureSpec> getFeatureSpecs(Iterable<String> featureIds) {
      Set<String> featureIdsSet = Sets.newHashSet(featureIds);
      return Maps.newHashMap(
          Maps.filterEntries(featureSpecs, (entry) -> featureIdsSet.contains(entry.getKey())));
    }

    @Override
    public Map<String, FeatureSpec> getAllFeatureSpecs() {
      return featureSpecs;
    }

    @Override
    public Map<String, StorageSpec> getStorageSpecs(Iterable<String> storageIds) {
      Set<String> storageIdsSet = Sets.newHashSet(storageIds);
      return Maps.newHashMap(
          Maps.filterEntries(storageSpecs, (entry) -> storageIdsSet.contains(entry.getKey())));
    }

    @Override
    public Map<String, StorageSpec> getAllStorageSpecs() {
      return storageSpecs;
    }
  }
}
