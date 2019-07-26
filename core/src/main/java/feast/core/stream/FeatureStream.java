package feast.core.stream;

import org.apache.kafka.common.errors.TopicExistsException;

public interface FeatureStream {

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
   * @param entityName name of entity whose features will be written to this topic
   * @return generated topic name
   */
  String generateTopicName(String entityName);
}
