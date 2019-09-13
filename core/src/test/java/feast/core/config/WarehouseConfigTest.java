package feast.core.config;

import static feast.core.config.StorageConfig.DEFAULT_WAREHOUSE_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.config.WarehouseConfig.WarehouseSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import org.junit.Test;

public class WarehouseConfigTest {

  @Test
  public void testGetMissingWarehouseSpec() {
    WarehouseConfig warehouseConfig = new WarehouseConfig();
    WarehouseSpec warehouseSpec = warehouseConfig.getMissingWarehouseSpec();
    assertNotNull(warehouseSpec);
    assertNotNull(warehouseSpec.getTrainingTemplateResource());
    assertNotNull(warehouseSpec.getViewTemplateResource());
  }

  @Test
  public void testGetBigQueryWarehouseSpec() {
    WarehouseConfig warehouseConfig = new WarehouseConfig();
    StorageSpec storageSpec = StorageSpec.newBuilder()
      .setId(DEFAULT_WAREHOUSE_ID).setType("bigquery")
      .putOptions("project", "project1")
      .putOptions("dataset", "dataset1")
      .build();
    StorageSpecs storageSpecs = StorageSpecs.builder()
      .warehouseStorageSpec(storageSpec)
      .build();
    WarehouseSpec warehouseSpec = warehouseConfig.getBigQueryWarehouseSpec(storageSpecs);
    assertEquals(warehouseSpec.getViewTemplateResource(), "templates/bq_view.tmpl");
    assertEquals(warehouseSpec.getTrainingTemplateResource(), "templates/bq_training.tmpl");
    assertEquals(warehouseSpec.createLinkUrl("entity1"),
      "https://bigquery.cloud.google.com/table/project1:dataset1.entity1_view");
    assertEquals(warehouseSpec.createTableId("entity1"), "project1.dataset1.entity1");
  }
}
