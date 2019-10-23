package feast.ingestion.transform.fn;

import feast.ingestion.values.FailedElement;
import feast.ingestion.values.Field;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FeatureRowProto.FeatureRow.Builder;
import feast.types.FieldProto;
import feast.types.ValueProto.Value;
import feast.types.ValueProto.ValueType.Enum;
import java.util.HashMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Rule;
import org.junit.Test;

public class KafkaRecordToFeatureRowDoFnTest {

  private static final TupleTag<FeatureRow> FEATURE_ROW_OUT = new TupleTag<FeatureRow>() {
  };

  private static final TupleTag<FailedElement> DEADLETTER_OUT = new TupleTag<FailedElement>() {
  };

  @Rule
  public transient TestPipeline p = TestPipeline.create();


  @Test
  public void shouldOutputInvalidRowsWithFailureTag() {
    FeatureRow frWithStringVal = dummyFeatureRow("invalid:1", "field1", "field2")
        .toBuilder()
        .setFields(0, FieldProto.Field.newBuilder()
            .setName("field1").setValue(Value.newBuilder()
                .setStringVal("hi").build()))
        .build();

    Values<KafkaRecord<byte[], byte[]>> featureRows = Create
        .of(kafkaRecordOf(dummyFeatureRow("invalid:1", "field1", "field2")), // invalid featureset name
            kafkaRecordOf(frWithStringVal), // invalid field type
            kafkaRecordOf(dummyFeatureRow("valid:1", "field1", "field2", "field3"))) // invalid fields
        .withCoder(KafkaRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()));

    HashMap<String, Field> fieldByName = new HashMap<>();
    fieldByName.put("field1", new Field("field1", Enum.INT64));
    fieldByName.put("field2", new Field("field2", Enum.INT64));

    PCollectionTuple output = p.apply(featureRows)
        .apply(ParDo.of(KafkaRecordToFeatureRowDoFn.newBuilder()
            .setFieldByName(fieldByName)
            .setSuccessTag(FEATURE_ROW_OUT)
            .setSuccessTag(FEATURE_ROW_OUT)
            .setFailureTag(DEADLETTER_OUT)
            .setFeatureSetName("valid")
            .setFeatureSetVersion(1)
            .build()).withOutputTags(FEATURE_ROW_OUT, TupleTagList.of(DEADLETTER_OUT)));

    PAssert.that(output.get(FEATURE_ROW_OUT)).empty();
    PAssert.thatSingleton(output.get(DEADLETTER_OUT).apply(Count.globally())).isEqualTo(3L);
    p.run();
  }

  @Test
  public void shouldOutputValidRowsWithSuccessTag() {
    Values<KafkaRecord<byte[], byte[]>> featureRows = Create
        .of(kafkaRecordOf(dummyFeatureRow("valid:1", "field1", "field2")),
            kafkaRecordOf(dummyFeatureRow("valid:1", "field1", "field2")),
            kafkaRecordOf(dummyFeatureRow("valid:1", "field1", "field2")))
        .withCoder(KafkaRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()));

    HashMap<String, Field> fieldByName = new HashMap<>();
    fieldByName.put("field1", new Field("field1", Enum.INT64));
    fieldByName.put("field2", new Field("field2", Enum.INT64));

    PCollectionTuple output = p.apply(featureRows)
        .apply(ParDo.of(KafkaRecordToFeatureRowDoFn.newBuilder()
            .setFieldByName(fieldByName)
            .setSuccessTag(FEATURE_ROW_OUT)
            .setFailureTag(DEADLETTER_OUT)
            .setFeatureSetName("valid")
            .setFeatureSetVersion(1)
            .build()).withOutputTags(FEATURE_ROW_OUT, TupleTagList.of(DEADLETTER_OUT)));


    PAssert.that(output.get(DEADLETTER_OUT)).empty();
    PAssert.thatSingleton(output.get(FEATURE_ROW_OUT).apply(Count.globally())).isEqualTo(3L);
    p.run();
  }

  private FeatureRow dummyFeatureRow(String featureSet, String... fieldNames) {
    Builder builder = FeatureRow.newBuilder().setFeatureSet(featureSet);
    for (String fieldName : fieldNames) {
      builder.addFields(FieldProto.Field.newBuilder()
          .setName(fieldName)
          .setValue(Value.newBuilder()
              .setInt64Val(1).build()).build());
    }
    return builder.build();
  }

  private KafkaRecord<byte[], byte[]> kafkaRecordOf(FeatureRow featureRow) {
    ConsumerRecord cr = new ConsumerRecord("", 1, 1, new byte[0],
        featureRow.toByteArray());
    return new KafkaRecord(cr.topic(), cr.partition(), cr.offset(), 0,
        KafkaTimestampType.NO_TIMESTAMP_TYPE, cr.headers(), cr.key(), cr.value());
  }
}