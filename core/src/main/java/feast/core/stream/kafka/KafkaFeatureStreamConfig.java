package feast.core.stream.kafka;

import feast.core.util.TypeConversion;
import java.util.Map;
import javax.naming.ConfigurationException;
import lombok.Value;

@Value
public class KafkaFeatureStreamConfig {

  private static String NUM_PARTITIONS_DEFAULT = "1";
  private static String REPLICATION_FACTOR_DEFAULT = "1";

  /**
   * Feast stream kafka bootstrap servers
   */
  String bootstrapServers;

  /**
   * Feast stream topic name
   */
  String topic;

  /**
   * Number of partitions per topic
   */
  int topicNumPartitions;

  /**
   * Replication factor of each topic created
   */
  short topicReplicationFactor;

  /**
   * Constructor from a map
   *
   * @param optionMap map containing the kafka feature stream configuration in key:value
   * format. The options should contain:
   * 1. bootstrapServers: optional, default kafka bootstrap servers, defaults to "KAFKA:9092" <br>
   * 2. topicPrefix: optional, topic prefix, defaults to "feast" <br>
   * 3. partitions: optional, number of partitions per topic created, defaults to 10 <br>
   * 4. replicationFactor: optional, replication factor of topics created, defaults to 2 <br>
   *
   * @return KafkaFeatureStreamConfig object
   */
  public static KafkaFeatureStreamConfig fromMap(Map<String, String> optionMap) {
    String bootstrapServers = optionMap.getOrDefault("bootstrapServers", "KAFKA:9092");
    String topicPrefix = optionMap.getOrDefault("topic", "feast-features");
    int numPartitions = Integer.parseInt(optionMap.getOrDefault("partitions", NUM_PARTITIONS_DEFAULT));
    short replicationFactor = Short.parseShort(optionMap.getOrDefault("replicationFactor", REPLICATION_FACTOR_DEFAULT));

    return new KafkaFeatureStreamConfig(bootstrapServers, topicPrefix, numPartitions, replicationFactor);
  }
}
