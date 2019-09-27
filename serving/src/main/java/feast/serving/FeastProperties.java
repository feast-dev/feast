package feast.serving;

// Feast configuration properties that maps Feast configuration from default application.yml file to
// a Java object.
// https://www.baeldung.com/configuration-properties-in-spring-boot
// https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-typesafe-configuration-properties

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@ConfigurationProperties(prefix = "feast")
public class FeastProperties {
  private String version;
  private String coreHost;
  private int corePort;
  private String storeName;
  private boolean tracingEnabled;
  private String tracingTracerName;
  private String tracingServiceName;
}
