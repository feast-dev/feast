package feast.serving.configuration;

import feast.serving.service.RedisServingService;
import feast.serving.service.ServingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ServingServiceConfig {
  @Bean
  public ServingService servingService() {
    return new RedisServingService();
  }
}
