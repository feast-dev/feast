package feast.serving.service;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class CachedSpecStorageTest {

  private CoreService coreService;
  private CachedSpecStorage cachedSpecStorage;

  @Before
  public void setUp() throws Exception {
    coreService = mock(CoreService.class);
    cachedSpecStorage = new CachedSpecStorage(coreService);
  }

  @Test
  public void testPopulateCache() {
    Map<String, FeatureSpec> featureSpecMap = new HashMap<>();
    featureSpecMap.put("feature_1", mock(FeatureSpec.class));
    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    entitySpecMap.put("entity_1", mock(EntitySpec.class));

    when(coreService.getAllFeatureSpecs()).thenReturn(featureSpecMap);
    when(coreService.getAllEntitySpecs()).thenReturn(entitySpecMap);
    cachedSpecStorage.populateCache();

    Map<String, FeatureSpec> result =
        cachedSpecStorage.getFeatureSpecs(Collections.singletonList("feature_1"));
    Map<String, EntitySpec> result2 =
        cachedSpecStorage.getEntitySpecs(Collections.singletonList("entity_1"));

    assertThat(result.size(), equalTo(1));
    assertThat(result2.size(), equalTo(1));

    verify(coreService, times(0)).getFeatureSpecs(any(Iterable.class));
    verify(coreService, times(0)).getEntitySpecs(any(Iterable.class));
  }


  @Test
  public void reloadFailureShouldReturnOldValue() {
    Map<String, FeatureSpec> featureSpecMap = new HashMap<>();
    featureSpecMap.put("feature_1", mock(FeatureSpec.class));

    Map<String, StorageSpec> storageSpecMap = new HashMap<>();
    storageSpecMap.put("storage_1", mock(StorageSpec.class));

    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    entitySpecMap.put("entity_1", mock(EntitySpec.class));

    when(coreService.getAllFeatureSpecs()).thenReturn(featureSpecMap);
    when(coreService.getFeatureSpecs(any(Iterable.class))).thenThrow(new RuntimeException("error"));
    when(coreService.getAllEntitySpecs()).thenReturn(entitySpecMap);
    when(coreService.getEntitySpecs(any(Iterable.class))).thenThrow(new RuntimeException("error"));

    cachedSpecStorage.populateCache();
    Map<String, FeatureSpec> result =
        cachedSpecStorage.getFeatureSpecs(Collections.singletonList("feature_1"));
    Map<String, EntitySpec> result2 =
        cachedSpecStorage.getEntitySpecs(Collections.singletonList("entity_1"));

    assertThat(result.size(), equalTo(1));
    assertThat(result2.size(), equalTo(1));
    verify(coreService, times(0)).getFeatureSpecs(any(Iterable.class));
    verify(coreService, times(0)).getEntitySpecs(any(Iterable.class));

    result = cachedSpecStorage.getFeatureSpecs(Collections.singletonList("feature_1"));
    result2 = cachedSpecStorage.getEntitySpecs(Collections.singletonList("entity_1"));
    assertThat(result.size(), equalTo(1));
    assertThat(result2.size(), equalTo(1));
  }

  @Test
  public void whenPopulateCache_shouldCacheServingStorageSpec() {
    Map<String, FeatureSpec> featureSpecMap = new HashMap<>();
    featureSpecMap.put("feature_1", mock(FeatureSpec.class));

    Map<String, StorageSpec> storageSpecMap = new HashMap<>();
    storageSpecMap.put("storage_1", mock(StorageSpec.class));

    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    entitySpecMap.put("entity_1", mock(EntitySpec.class));

    StorageSpec storageSpec = StorageSpec.newBuilder().setId("SERVING").build();

    when(coreService.getAllFeatureSpecs()).thenReturn(featureSpecMap);
    when(coreService.getAllEntitySpecs()).thenReturn(entitySpecMap);
    when(coreService.getServingStorageSpec())
        .thenReturn(storageSpec);

    cachedSpecStorage.populateCache();
    assertEquals(storageSpec, cachedSpecStorage.getServingStorageSpec());
    assertEquals(storageSpec, cachedSpecStorage.getServingStorageSpec());
    verify(coreService, times(1)).getServingStorageSpec();
  }
}