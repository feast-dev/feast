package feast.store.serving.redis;

import static org.junit.Assert.*;

import com.google.protobuf.Timestamp;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.ingestion.transform.ValidateFeatureRows;
import feast.storage.RedisProto.RedisKey;
import feast.store.serving.redis.RedisCustomIO.Method;
import feast.store.serving.redis.RedisCustomIO.RedisMutation;
import feast.test.TestUtil;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FieldProto.Field;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FeatureRowToRedisMutationDoFnTest {

  @Rule
  public transient TestPipeline p = TestPipeline.create();

  private FeatureSetSpec fs = FeatureSetSpec.newBuilder()
      .setName("feature_set")
      .setVersion(1)
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

  @Test
  public void shouldConvertRowWithDuplicateEntitiesToValidKey() {
    Map<String, FeatureSetSpec> featureSetSpecs = new HashMap<>();
    featureSetSpecs.put("feature_set", fs);

    FeatureRow offendingRow = FeatureRow.newBuilder()
        .setFeatureSet("feature_set")
        .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
        .addFields(Field.newBuilder().setName("entity_id_primary")
            .setValue(Value.newBuilder().setInt32Val(1)))
        .addFields(Field.newBuilder().setName("entity_id_primary")
            .setValue(Value.newBuilder().setInt32Val(2)))
        .addFields(Field.newBuilder().setName("entity_id_secondary")
            .setValue(Value.newBuilder().setStringVal("a")))
        .build();

    PCollection<RedisMutation> output = p
        .apply(Create.of(Collections.singletonList(offendingRow)))
        .setCoder(ProtoCoder.of(FeatureRow.class))
        .apply(ParDo.of(new FeatureRowToRedisMutationDoFn(featureSetSpecs)));

    RedisKey expectedKey = RedisKey.newBuilder()
        .setFeatureSet("feature_set")
        .addEntities(Field.newBuilder().setName("entity_id_primary")
            .setValue(Value.newBuilder().setInt32Val(1)))
        .addEntities(Field.newBuilder().setName("entity_id_secondary")
            .setValue(Value.newBuilder().setStringVal("a")))
        .build();

    PAssert.that(output).satisfies((SerializableFunction<Iterable<RedisMutation>, Void>) input -> {
      input.forEach(rm -> {
        assert(Arrays.equals(rm.getKey(), expectedKey.toByteArray()));
        assert(Arrays.equals(rm.getValue(), offendingRow.toByteArray()));
      });
      return null;
    });
    p.run();
  }

  @Test
  public void shouldConvertRowWithOutOfOrderEntitiesToValidKey() {
    Map<String, FeatureSetSpec> featureSetSpecs = new HashMap<>();
    featureSetSpecs.put("feature_set", fs);

    FeatureRow offendingRow = FeatureRow.newBuilder()
        .setFeatureSet("feature_set")
        .setEventTimestamp(Timestamp.newBuilder().setSeconds(10))
        .addFields(Field.newBuilder().setName("entity_id_secondary")
            .setValue(Value.newBuilder().setStringVal("a")))
        .addFields(Field.newBuilder().setName("entity_id_primary")
            .setValue(Value.newBuilder().setInt32Val(1)))
        .build();

    PCollection<RedisMutation> output = p
        .apply(Create.of(Collections.singletonList(offendingRow)))
        .setCoder(ProtoCoder.of(FeatureRow.class))
        .apply(ParDo.of(new FeatureRowToRedisMutationDoFn(featureSetSpecs)));

    RedisKey expectedKey = RedisKey.newBuilder()
        .setFeatureSet("feature_set")
        .addEntities(Field.newBuilder().setName("entity_id_primary")
            .setValue(Value.newBuilder().setInt32Val(1)))
        .addEntities(Field.newBuilder().setName("entity_id_secondary")
            .setValue(Value.newBuilder().setStringVal("a")))
        .build();

    PAssert.that(output).satisfies((SerializableFunction<Iterable<RedisMutation>, Void>) input -> {
      input.forEach(rm -> {
        assert(Arrays.equals(rm.getKey(), expectedKey.toByteArray()));
        assert(Arrays.equals(rm.getValue(), offendingRow.toByteArray()));
      });
      return null;
    });
    p.run();
  }

}