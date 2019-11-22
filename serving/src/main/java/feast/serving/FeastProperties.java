package feast.serving;

// Feast configuration properties that maps Feast configuration from default application.yml file to
// a Java object.
// https://www.baeldung.com/configuration-properties-in-spring-boot
// https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-typesafe-configuration-properties

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "feast")
public class FeastProperties {
  private String version;
  private String coreHost;
  private int coreGrpcPort;
  private StoreProperties store;
  private JobProperties jobs;
  private TracingProperties tracing;

  @Setter
  @Getter
  public static class StoreProperties {
    private String configPath;
    private int redisPoolMaxSize;
    private int redisPoolMaxIdle;
  }

  @Setter
  @Getter
  public static class JobProperties {
    private String stagingLocation;
    private String storeType;
    private Map<String, String> storeOptions;
  }

  @Setter
  @Getter
  public static class TracingProperties {
    private boolean enabled;
    private String tracerName;
    private String serviceName;
  }
}
