package feast.serving.configuration;

import feast.serving.FeastProperties;
import feast.serving.service.CachedSpecService;
import feast.serving.service.CoreSpecService;
import feast.serving.service.SpecService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SpecServiceConfig {
  private String feastCoreHost;
  private int feastCorePort;
  private static final int CACHE_REFRESH_RATE_MINUTES = 30;

  private ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();

  @Autowired
  public SpecServiceConfig(FeastProperties feastProperties) {
    feastCoreHost = feastProperties.getCoreHost();
    feastCorePort = feastProperties.getCorePort();
  }

  @Bean
  public SpecService specService(FeastProperties feastProperties) {
    CoreSpecService coreService = new CoreSpecService(feastCoreHost, feastCorePort);
    CachedSpecService cachedSpecStorage =
        new CachedSpecService(coreService, feastProperties.getStoreName());
    // reload all specs including new ones periodically
    scheduledExecutorService.schedule(
        cachedSpecStorage::populateCache, CACHE_REFRESH_RATE_MINUTES, TimeUnit.MINUTES);

    try {
      cachedSpecStorage.populateCache();
    } catch (Exception e) {
      log.error("Unable to preload feast's spec");
    }
    return cachedSpecStorage;
  }
}
