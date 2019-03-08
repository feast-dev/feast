package feast.core;

import static feast.core.config.StorageConfig.DEFAULT_SERVING_ID;
import static feast.core.config.StorageConfig.DEFAULT_WAREHOUSE_ID;
import static org.junit.Assert.assertEquals;

import feast.core.model.StorageInfo;
import feast.core.service.SpecService;
import feast.specs.StorageSpecProto.StorageSpec;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Starts the application context with some properties
 */
@RunWith(SpringRunner.class)
@SpringBootTest(properties = {
    "feast.jobs.workspace=${java.io.tmpdir}/jobs",
    "spring.datasource.url=jdbc:h2:mem:testdb",
    "feast.store.warehouse.type=file.json",
    "feast.store.warehouse.options={\"path\":\"${java.io.tmpdir}warehouse\"}",
    "feast.store.serving.type=redis",
    "feast.store.serving.options={\"host\":\"localhost\",\"port\":1234}"
})
public class CoreApplicationTest {

  @Autowired
  SpecService specService;

  @Test
  public void test_withProperties_systemServingAndWarehouseStoresRegistered() {
    List<StorageInfo> warehouseStorageInfo = specService
        .getStorage(Collections.singletonList(DEFAULT_WAREHOUSE_ID));
    assertEquals(warehouseStorageInfo.size(), 1);
    assertEquals(warehouseStorageInfo.get(0).getStorageSpec(), StorageSpec.newBuilder()
        .setId(DEFAULT_WAREHOUSE_ID).setType("file.json").putOptions("path",
            Paths.get(System.getProperty("java.io.tmpdir")).resolve("warehouse").toString())
        .build());

    List<StorageInfo> servingStorageInfo = specService
        .getStorage(Collections.singletonList(DEFAULT_SERVING_ID));
    assertEquals(warehouseStorageInfo.size(), 1);
    assertEquals(servingStorageInfo.get(0).getStorageSpec(), StorageSpec.newBuilder()
        .setId(DEFAULT_SERVING_ID).setType("redis")
        .putOptions("host", "localhost")
        .putOptions("port", "1234")
        .build());
  }
}