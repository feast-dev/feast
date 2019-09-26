package feast.serving.configuration;

import feast.serving.service.CoreSpecService;
import feast.serving.service.SpecService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SpecServiceConfig {
  @Value("${feast.core.host}")
  private String feastCoreHost;

  @Value("${feast.core.port}")
  private int feastCorePort;

  @Bean
  public SpecService specService() {
    return new CoreSpecService(feastCoreHost, feastCorePort);
  }
}
