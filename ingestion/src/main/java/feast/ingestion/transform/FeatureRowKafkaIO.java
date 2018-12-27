package feast.ingestion.transform;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import feast.ingestion.deserializer.FeatureRowDeserializer;
import feast.ingestion.deserializer.FeatureRowKeyDeserializer;
import feast.options.OptionsParser;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.FeatureRowProto.FeatureRowKey;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

import static com.google.common.base.Preconditions.checkArgument;

public class FeatureRowKafkaIO {

    static final String KAFKA_TYPE = "kafka";

    public static Read read(ImportSpec importSpec) {
        return new Read(importSpec);
    }

    public static class Read extends FeatureIO.Read {

        private ImportSpec importSpec;

        private Read(ImportSpec importSpec) {
            this.importSpec = importSpec;
        }

        @Override
        public PCollection<FeatureRow> expand(PInput input) {

            checkArgument(importSpec.getType().equals(KAFKA_TYPE));

            String bootstrapServer = importSpec.getOptionsMap().get("server");

            Preconditions.checkArgument(
                    !Strings.isNullOrEmpty(bootstrapServer), "kafka bootstrap server must be set");

            String topic = importSpec.getOptionsMap().get("topic");

            Preconditions.checkArgument(
                    !Strings.isNullOrEmpty(topic), "kafka topic must be set");

            KafkaIO.Read<FeatureRowKey, FeatureRow> kafkaIOReader = KafkaIO.<FeatureRowKey, FeatureRow>read()
                    .withBootstrapServers(bootstrapServer)
                    .withTopic(topic)
                    .withKeyDeserializer(FeatureRowKeyDeserializer.class)
                                    .withValueDeserializer(FeatureRowDeserializer.class);

            PCollection<KafkaRecord<FeatureRowKey, FeatureRow>> featureRowRecord = input.getPipeline().apply(kafkaIOReader);

            PCollection<FeatureRow> featureRow = featureRowRecord.apply(
                    ParDo.of(
                            new DoFn<KafkaRecord<FeatureRowKey, FeatureRow>, FeatureRow>() {
                                @ProcessElement
                                public void processElement(ProcessContext processContext) {
                                    KafkaRecord<FeatureRowKey, FeatureRow> record = processContext.element();
                                    processContext.output(record.getKV().getValue());
                                }
                            }));
            return featureRow;
        }
    }
}
