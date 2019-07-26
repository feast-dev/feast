package feast.core.stream.kafka;

import feast.core.util.TypeConversion;
import java.util.Map;
import javax.naming.ConfigurationException;
import lombok.Value;

@Value
public class KafkaFeatureStreamConfig {

  private static String NUM_PARTITIONS_DEFAULT = "10";
  private static String REPLICATION_FACTOR_DEFAULT = "2";

  /**
   * Kafka Bootstrap servers, comma separated
   */
  String bootstrapServers;

  /**
   * Feast stream topic prefix, to be prepended to topic name
   */
  String topicPrefix;

  /**
   * Number of partitions per topic
   */
  int topicNumPartitions;

  /**
   * Replication factor of each topic created
   */
  short topicReplicationFactor;

  /**
   * Constructor from a valid JSON string
   *
   * @param options JSON string containing the kafka feature stream configuration in key:value
   * format. The options should contain:
   * 1. bootstrapServers: compulsory, kafka bootstrap servers, comma separated <br>
   * 2. topicPrefix: optional, topic prefix, defaults to "feast" <br>
   * 3. partitions: optional, number of partitions per topic created, defaults to 10 <br>
   * 4. replicationFactor: optional, replication factor of topics created, defaults to 2 <br>
   *
   * @return KafkaFeatureStreamConfig object
   */
  public static KafkaFeatureStreamConfig fromJSON(String options) throws ConfigurationException {
    Map<String, String> optionMap = TypeConversion.convertJsonStringToMap(options);
    String bootstrapServers = optionMap.get("bootstrapServers");
    if (bootstrapServers == null) {
      throw new ConfigurationException(
          "option bootstrapServers not set in feast.stream.options. Bootstrap servers required to connect to feast stream");
    }
    String topicPrefix = optionMap.getOrDefault("topicPrefix", "feast");
    int numPartitions = Integer.parseInt(optionMap.getOrDefault("partitions", NUM_PARTITIONS_DEFAULT));
    short replicationFactor = Short.parseShort(optionMap.getOrDefault("replicationFactor", REPLICATION_FACTOR_DEFAULT));

    return new KafkaFeatureStreamConfig(bootstrapServers, topicPrefix, numPartitions, replicationFactor);
  }
}
