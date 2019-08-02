package feast.core.service;

import com.google.common.base.Strings;
import feast.core.dao.FeatureStreamTopicRepository;
import feast.core.exception.TopicExistsException;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureStreamTopic;
import feast.core.stream.FeatureStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Facilitates management of the feature stream.
 */
@Slf4j
@Service
public class FeatureStreamService {

  private final FeatureStreamTopicRepository featureStreamTopicRepository;
  private final FeatureStream featureStream;

  @Autowired
  public FeatureStreamService(FeatureStreamTopicRepository featureStreamTopicRepository,
      FeatureStream featureStream) {
    this.featureStreamTopicRepository = featureStreamTopicRepository;
    this.featureStream = featureStream;
  }

  /**
   * Provisions a topic given the entity. If the topic already exists, and was not created by feast,
   * an error will be thrown.
   *
   * @param entity entity to create the topic for
   * @return created topic
   */
  public FeatureStreamTopic provisionTopic(EntityInfo entity) {
    String topicName = featureStream.generateTopicName(entity.getName());
    try {
      featureStream.provisionTopic(topicName);
    } catch (TopicExistsException e) {
      if (!featureStreamTopicRepository.existsById(topicName)) {
        // topic exists, and we didn't create it, throw error
        throw new TopicExistsException(e.getMessage(), e);
      }
    }
    FeatureStreamTopic topic = new FeatureStreamTopic(topicName, entity);
    featureStreamTopicRepository.saveAndFlush(topic);
    return topic;
  }

  /**
   * Deletes a topic from the stream. If the topic was not created by feast, and exception will be
   * thrown.
   *
   * @param topic topic to delete
   */
  public void deleteTopic(FeatureStreamTopic topic) {
    if (!featureStreamTopicRepository.existsById(topic.getName())) {
      throw new IllegalArgumentException(Strings
          .lenientFormat("Could not delete %s: Unable to delete topic not created by feast",
              topic.getName()));
    }
    featureStream.deleteTopic(topic.getName());
    featureStreamTopicRepository.delete(topic);
  }

  /**
   * Get the topic the given entity should write to
   *
   * @return FeatureStreamTopic object containing the name of the topic
   */
  public FeatureStreamTopic getTopicFor(EntityInfo entity) {
    FeatureStreamTopic topic = featureStreamTopicRepository.findByEntityName(entity.getName());
    if (topic == null) {
      throw new IllegalArgumentException(Strings
          .lenientFormat("Topic not created for entity %s, has the entity been registered?",
              entity.getName()));
    }
    return topic;
  }

  /**
   * Get the feature stream broker URI
   *
   * @return broker URI, comma separated
   */
  public String getBrokerUri() {
    return featureStream.getBrokerUri();
  }

}
