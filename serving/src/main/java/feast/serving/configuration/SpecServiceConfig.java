package feast.serving.configuration;

import feast.serving.FeastProperties;
import feast.serving.service.CachedSpecService;
import feast.serving.service.CoreSpecService;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  private static final int CACHE_REFRESH_RATE_MINUTES = 1;

  @Autowired
  public SpecServiceConfig(FeastProperties feastProperties) {
    feastCoreHost = feastProperties.getCoreHost();
    feastCorePort = feastProperties.getCoreGrpcPort();
  }

  @Bean
  public ScheduledExecutorService cachedSpecServiceScheduledExecutorService(
      CachedSpecService cachedSpecStorage) {
    ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor();
    // reload all specs including new ones periodically
    scheduledExecutorService.scheduleAtFixedRate(
        cachedSpecStorage::scheduledPopulateCache,
        CACHE_REFRESH_RATE_MINUTES,
        CACHE_REFRESH_RATE_MINUTES,
        TimeUnit.MINUTES);
    return scheduledExecutorService;
  }

  @Bean
  public CachedSpecService specService(FeastProperties feastProperties) {

    CoreSpecService coreService = new CoreSpecService(feastCoreHost, feastCorePort);
    Path path = Paths.get(feastProperties.getStore().getConfigPath());
    CachedSpecService cachedSpecStorage = new CachedSpecService(coreService, path);
    try {
      cachedSpecStorage.populateCache();
    } catch (Exception e) {
      log.error("Unable to preload feast's spec");
    }
    return cachedSpecStorage;
  }
}
