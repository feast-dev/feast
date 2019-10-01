package feast.core.service;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.SourceProto.KafkaSourceConfig;
import feast.core.config.FeastProperties;
import feast.core.model.FeatureSet;
import feast.core.model.Source;
import feast.core.stream.FeatureStream;
import feast.core.stream.kafka.KafkaFeatureStream;
import feast.core.stream.kafka.KafkaFeatureStreamConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Facilitates management of the feature stream.
 */
@Slf4j
@Service
public class FeatureStreamService {
  private final String defaultOptions;

  @Autowired
  public FeatureStreamService(FeastProperties feastProperties) {
    this.defaultOptions = new Gson().toJson(feastProperties.getStream().getOptions());
  }

  /**
   * Provisions a topic given the featureSet. If the topic already exists, and was not created by
   * feast, an error will be thrown.
   *
   * @param featureSet featureSet to create the topic for
   * @return Source updated with provisioned feature source
   */
  public Source setUpSource(FeatureSet featureSet) {
    try {
      switch (featureSet.getSource().getType()) {
        case KAFKA:
          KafkaSourceConfig options = (KafkaSourceConfig) featureSet.getSource().getOptions();
          Map<String, Object> map = new HashMap<>();
          map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
          map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
          AdminClient client = AdminClient.create(map);
          FeatureStream featureStream = new KafkaFeatureStream(client,
              KafkaFeatureStreamConfig.fromJSON(defaultOptions));
          return featureStream.provision(featureSet);
        case UNRECOGNIZED:
        default:
          throw new IllegalArgumentException(
              String.format("Invalid source %s provided, only source of type [KAFKA] allowed",
                  featureSet.getSource().getType()));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format("Unable to set up source for featureSet %s", featureSet.getName()), e);
    }
  }

}
