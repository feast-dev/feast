package feast.core;

import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.storage.SchemaManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OnContextRefresh {

  @Autowired
  private SchemaManager schemaManager;
  @Autowired
  private StorageSpecs storageSpecs;

  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent event) {
    if (storageSpecs.getServingStorageSpec() != null) {
      schemaManager.registerStorage(storageSpecs.getServingStorageSpec());
    }
    if (storageSpecs.getWarehouseStorageSpec() != null) {
      schemaManager.registerStorage(storageSpecs.getWarehouseStorageSpec());
    }
  }
}