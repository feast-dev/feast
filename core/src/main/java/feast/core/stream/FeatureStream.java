package feast.core.stream;

import java.util.Map;
import org.apache.kafka.common.errors.TopicExistsException;

public interface FeatureStream {

  /**
   * Gets the type of feature stream
   * @return type of feature stream
   */
  String getType();

  /**
   * Gets options for connecting to the feature stream. Maps directly to the import job
   * spec's sourceOptions map.
   *
   * @return Map<String, String> of feature stream options
   */
  Map<String, String> getFeatureStreamOptions();

  /**
   * Provisions a topic in the feast stream for the given topic name.
   *
   * @param topicName name of the topic to create
   */
  void provisionTopic(String topicName) throws RuntimeException;

  void deleteTopic(String topicName);

  /**
   * Generates a topic name given the entity name
   *
   * @param featureSetName name of entity whose features will be written to this topic
   * @return generated topic name
   */
  String generateTopicName(String featureSetName);

  /**
   * Get the broker URI for this feature stream
   * @return broker URI for the feature stream
   */
  String getBrokerUri();
}
