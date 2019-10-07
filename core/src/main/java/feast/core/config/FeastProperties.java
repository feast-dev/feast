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
  private StatsdProperties statsd;

  @Getter
  @Setter
  public static class JobProperties {

    private String runner;
    private Map<String, String> options;
    private String dataflowProjectId;
    private String dataflowLocation;
  }

  @Getter
  @Setter
  public static class StreamProperties {

    private String type;
    private Map<String, String> options;
  }

  @Getter
  @Setter
  public static class StatsdProperties {

    private String host;
    private int port;
  }
}



