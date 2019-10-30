package feast.core.stream.kafka;

import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.SourceProto.SourceType;
import feast.core.model.FeatureSet;
import feast.core.model.Source;
import feast.core.stream.FeatureStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

@Slf4j
@AllArgsConstructor
public class KafkaFeatureStream implements FeatureStream {

  private static SourceType FEATURE_STREAM_TYPE = SourceType.KAFKA;

  private KafkaFeatureStreamConfig defaultConfig;

  @Override
  public SourceType getType() {
    return FEATURE_STREAM_TYPE;
  }

  @Override
  public Source provision(FeatureSet featureSet) throws RuntimeException {

    Source source = featureSet.getSource();
    KafkaSourceConfig config = KafkaSourceConfig.getDefaultInstance();
    String bootstrapServers = defaultConfig.getBootstrapServers();
    String topicName = defaultConfig.getTopic();
    if (!source.isUseDefault()) {
      try {
        config = (KafkaSourceConfig) source.getOptions();
        topicName = config.getTopic();
        bootstrapServers = config.getBootstrapServers();
      } catch (NullPointerException e) {
        throw new RuntimeException(String
            .format("Unable to retrieve bootstrap servers for featureSet %s", featureSet.getName()),
            e);
      }
    }

    Map<String, Object> map = new HashMap<>();
    map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
    AdminClient client = AdminClient.create(map);

    NewTopic newTopic = new NewTopic(topicName,
        defaultConfig.getTopicNumPartitions(),
        defaultConfig.getTopicReplicationFactor());
    CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
    try {
      createTopicsResult.values().get(topicName).get();
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause().getClass().equals(TopicExistsException.class)) {
        log.warn(Strings
            .lenientFormat(
                "Unable to create topic %s in the feature stream, topic already exists, using existing topic.",
                topicName));
      } else {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    assert config != null;
    source.setTopics(topicName);
    source.setBootstrapServers(bootstrapServers);
    return source;
  }
}
