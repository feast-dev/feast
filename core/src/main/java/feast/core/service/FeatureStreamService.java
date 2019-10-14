package feast.core.service;

import feast.core.config.FeastProperties;
import feast.core.model.FeatureSet;
import feast.core.model.Source;
import feast.core.stream.FeatureStream;
import feast.core.stream.kafka.KafkaFeatureStream;
import feast.core.stream.kafka.KafkaFeatureStreamConfig;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Facilitates management of the feature stream.
 */
@Slf4j
@Service
public class FeatureStreamService {
  private final Map<String, String> defaultOptions;

  @Autowired
  public FeatureStreamService(FeastProperties feastProperties) {
    this.defaultOptions = feastProperties.getStream().getOptions();
  }

  /**
   * Provisions a topic given the featureSet. If the topic already exists, and was not created by
   * feast, an error will be thrown.
   *
   * @param featureSet featureSet to create the topic for
   * @return Source updated with provisioned feature source
   */
  public Source setUpSource(FeatureSet featureSet) {
    switch (featureSet.getSource().getType()) {
      case KAFKA:
        FeatureStream featureStream = new KafkaFeatureStream(KafkaFeatureStreamConfig.fromMap(defaultOptions));
        return featureStream.provision(featureSet);
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format("Invalid source %s provided, only source of type [KAFKA] allowed",
                featureSet.getSource().getType()));
    }
  }
}
