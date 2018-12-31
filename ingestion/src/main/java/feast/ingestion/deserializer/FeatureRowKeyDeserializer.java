package feast.ingestion.deserializer;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.types.FeatureRowProto.FeatureRowKey;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializer for Kafka to deserialize Protocol Buffers messages
 *
 * @param <FeatureRowKey> Protobuf message type
 */
public class FeatureRowKeyDeserializer implements Deserializer<FeatureRowKey> {

  @Override
  public void configure(Map configs, boolean isKey) {}

  @Override
  public FeatureRowKey deserialize(String topic, byte[] data) {
    try {
      return FeatureRowKey.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException(
          "Error deserializing FeatureRowKey from Protobuf message", e);
    }
  }

  @Override
  public void close() {}
}
