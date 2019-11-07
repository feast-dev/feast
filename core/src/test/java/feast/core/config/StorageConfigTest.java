package feast.core.config;

import static feast.core.config.StorageConfig.DEFAULT_ERRORS_ID;
import static feast.core.config.StorageConfig.DEFAULT_SERVING_ID;
import static feast.core.config.StorageConfig.DEFAULT_WAREHOUSE_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;

import feast.core.config.StorageConfig.StorageSpecs;
import feast.core.validators.SpecValidator;
import feast.specs.StorageSpecProto.StorageSpec;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StorageConfigTest {

  public SpecValidator validator = Mockito.mock(SpecValidator.class);

  StorageConfig config;

  @Before
  public void before() {
    config = new StorageConfig(validator);
  }

  @Test
  public void testBuildErrorsStorageSpec() {
    StorageSpecs storageSpecs = config.getStorageSpecs("", "", "",
        "", "file.json", "{\"path\": \"/tmp/errors\"}");
    StorageSpec storageSpec = StorageSpec.newBuilder()
        .setId(DEFAULT_ERRORS_ID).setType("file.json")
        .putOptions("path", "/tmp/errors").build();
    assertEquals(storageSpec, storageSpecs.getErrorsStorageSpec());
    assertNull(storageSpecs.getServingStorageSpec());
    assertNull(storageSpecs.getWarehouseStorageSpec());
    verify(validator).validateErrorsStorageSpec(storageSpec);
  }

  @Test
  public void testBuildServingStorageSpec() {
    StorageSpecs storageSpecs = config.getStorageSpecs(
        "redis", "{\"host\": \"localhost\",  \"port\": \"1234\"}", "",
        "", "", "");
    StorageSpec storageSpec = StorageSpec.newBuilder()
        .setId(DEFAULT_SERVING_ID).setType("redis")
        .putOptions("host", "localhost")
        .putOptions("port", "1234").build();
    assertEquals(storageSpec, storageSpecs.getServingStorageSpec());
    assertNull(storageSpecs.getErrorsStorageSpec());
    assertNull(storageSpecs.getWarehouseStorageSpec());
    verify(validator).validateServingStorageSpec(storageSpec);
  }

  @Test
  public void testBuildWarehouseStorageSpec() {
    StorageSpecs storageSpecs = config.getStorageSpecs(
        "", "",
        "bigquery", "{\"project\": \"project1\", \"dataset\": \"feast\"}",
        "", "");
    StorageSpec storageSpec = StorageSpec.newBuilder()
        .setId(DEFAULT_WAREHOUSE_ID).setType("bigquery")
        .putOptions("project", "project1")
        .putOptions("dataset", "feast").build();
    assertEquals(storageSpec, storageSpecs.getWarehouseStorageSpec());
    assertNull(storageSpecs.getErrorsStorageSpec());
    assertNull(storageSpecs.getServingStorageSpec());
    verify(validator).validateWarehouseStorageSpec(storageSpec);
  }

  @Test
  public void testMine() {
    StorageSpecs storageSpecs = config.getStorageSpecs(
        "noop", "{}",
        "STORE_WAREHOUSE_TYPE", "{\"project\": \"dev-konnekt-data-deep-1\", \"dataset\": \"feast_warehouse\"}",
        "stdout", "");
  }
}
