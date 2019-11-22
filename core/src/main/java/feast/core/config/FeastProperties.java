package feast.core.config;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "feast", ignoreInvalidFields = true)
public class FeastProperties {

  private String version;
  private JobProperties jobs;
  private StreamProperties stream;

  @Getter
  @Setter
  public static class JobProperties {

    private String runner;
    private Map<String, String> options;
    private MetricsProperties metrics;
  }

  @Getter
  @Setter
  public static class StreamProperties {

    private String type;
    private Map<String, String> options;
  }

  @Getter
  @Setter
  public static class MetricsProperties {

    private boolean enabled;
    private String type;
    private String host;
    private int port;
  }
}
