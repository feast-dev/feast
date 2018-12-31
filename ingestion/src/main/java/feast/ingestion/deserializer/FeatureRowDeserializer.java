package feast.ingestion.deserializer;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.types.FeatureRowProto.FeatureRow;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializer for Kafka to deserialize Protocol Buffers messages
 *
 * @param <FeatureRow> Protobuf message type
 */
public class FeatureRowDeserializer implements Deserializer<FeatureRow> {

  @Override
  public void configure(Map configs, boolean isKey) {}

  @Override
  public FeatureRow deserialize(String topic, byte[] data) {
    try {
      return FeatureRow.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException("Error deserializing FeatureRow from Protobuf message", e);
    }
  }

  @Override
  public void close() {}
}
