package feast.serving.configuration;

import feast.serving.FeastProperties;
import feast.serving.service.CoreSpecService;
import feast.serving.service.SpecService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SpecServiceConfig {
  private String feastCoreHost;
  private int feastCorePort;

  @Autowired
  public SpecServiceConfig(FeastProperties feastProperties) {
    feastCoreHost = feastProperties.getCoreHost();
    feastCorePort = feastProperties.getCorePort();
  }

  @Bean
  public SpecService specService() {
    return new CoreSpecService(feastCoreHost, feastCorePort);
  }
}
