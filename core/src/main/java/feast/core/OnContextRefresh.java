package feast.core;

import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.service.SpecService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OnContextRefresh {

  @Autowired
  private SpecService specService;
  @Autowired
  private StorageSpecs storageSpecs;

  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent event) {
    if (storageSpecs.getServingStorageSpec() != null) {
      specService.registerStorage(storageSpecs.getServingStorageSpec());
    }
    if (storageSpecs.getWarehouseStorageSpec() != null) {
      specService.registerStorage(storageSpecs.getWarehouseStorageSpec());
    }
  }
}