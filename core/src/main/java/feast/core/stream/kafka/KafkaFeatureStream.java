package feast.core.stream.kafka;

import com.google.common.base.Strings;
import feast.core.exception.TopicExistsException;
import feast.core.stream.FeatureStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
@AllArgsConstructor
public class KafkaFeatureStream implements FeatureStream {

  private static String FEATURE_STREAM_TYPE = "kafka";

  private AdminClient client;
  private KafkaFeatureStreamConfig config;

  @Override
  public String getType() {
    return FEATURE_STREAM_TYPE;
  }

  @Override
  public Map<String, String> getFeatureStreamOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("discardUnknownFeatures", "true");
    options.put("bootstrapServers", config.getBootstrapServers());
    return options;
  }

  @Override
  public void provisionTopic(String topicName) throws RuntimeException {
    NewTopic newTopic = new NewTopic(topicName, config.getTopicNumPartitions(),
        config.getTopicReplicationFactor());
    CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
    try {
      createTopicsResult.values().get(topicName).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e.getMessage(), e);
    } catch (org.apache.kafka.common.errors.TopicExistsException e) {
      throw new TopicExistsException(Strings
          .lenientFormat("Unable to create topic %s in the feature stream, topic already exists.",
              topicName));
    }
  }

  @Override
  public void deleteTopic(String topicName) {
    client.deleteTopics(Collections.singleton(topicName));
  }

  @Override
  public String generateTopicName(String entityName) {
    return Strings.lenientFormat("%s-%s-features", config.getTopicPrefix(), entityName);
  }

  @Override
  public String getBrokerUri() {
    return config.getBootstrapServers();
  }

}
