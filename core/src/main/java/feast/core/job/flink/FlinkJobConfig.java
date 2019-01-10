package feast.core.job.flink;

import lombok.Value;

@Value
public class FlinkJobConfig {

  /**
   * Flink's job master URL
   * e.g: localhost:8081
   */
  String masterUrl;

  /**
   * Directory containing flink-conf.yaml
   * e.g.: /etc/flink/conf
   */
  String configDir;
}
