package feast.serving.service;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.FakeTicker;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class CachedSpecStorageTest {

  private FakeTicker fakeTicker;
  private CoreService coreService;
  private CachedSpecStorage cachedSpecStorage;

  @Before
  public void setUp() throws Exception {
    fakeTicker = new FakeTicker();
    coreService = mock(CoreService.class);
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
    cachedSpecStorage = new CachedSpecStorage(coreService, executorService,
        Duration.ofSeconds(5), fakeTicker);
  }

  @Test
  public void shouldNotBeNull() {
    assertNotNull(cachedSpecStorage);
  }

  @Test
  public void testPopulateCache() {
    Map<String, FeatureSpec> featureSpecMap = new HashMap<>();
    featureSpecMap.put("feature_1", mock(FeatureSpec.class));

    Map<String, StorageSpec> storageSpecMap = new HashMap<>();
    storageSpecMap.put("storage_1", mock(StorageSpec.class));

    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    entitySpecMap.put("entity_1", mock(EntitySpec.class));

    when(coreService.getAllFeatureSpecs()).thenReturn(featureSpecMap);
    when(coreService.getAllEntitySpecs()).thenReturn(entitySpecMap);
    when(coreService.getAllStorageSpecs()).thenReturn(storageSpecMap);

    cachedSpecStorage.populateCache();
    Map<String, FeatureSpec> result = cachedSpecStorage.getFeatureSpecs(Collections.singletonList("feature_1"));
    Map<String, StorageSpec> result1 = cachedSpecStorage.getStorageSpecs(Collections.singletonList("storage_1"));
    Map<String, EntitySpec> result2 = cachedSpecStorage.getEntitySpecs(Collections.singletonList("entity_1"));

    assertThat(result.size(), equalTo(1));
    assertThat(result1.size(), equalTo(1));
    assertThat(result2.size(), equalTo(1));

    verify(coreService, times(0)).getFeatureSpecs(any(Iterable.class));
    verify(coreService, times(0)).getStorageSpecs(any(Iterable.class));
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
    when(coreService.getAllStorageSpecs()).thenReturn(storageSpecMap);
    when(coreService.getStorageSpecs(any(Iterable.class))).thenThrow(new RuntimeException("error"));


    cachedSpecStorage.populateCache();
    Map<String, FeatureSpec> result = cachedSpecStorage.getFeatureSpecs(Collections.singletonList("feature_1"));
    Map<String, StorageSpec> result1 = cachedSpecStorage.getStorageSpecs(Collections.singletonList("storage_1"));
    Map<String, EntitySpec> result2 = cachedSpecStorage.getEntitySpecs(Collections.singletonList("entity_1"));

    assertThat(result.size(), equalTo(1));
    assertThat(result1.size(), equalTo(1));
    assertThat(result2.size(), equalTo(1));
    verify(coreService, times(0)).getFeatureSpecs(any(Iterable.class));
    verify(coreService, times(0)).getStorageSpecs(any(Iterable.class));
    verify(coreService, times(0)).getEntitySpecs(any(Iterable.class));

    fakeTicker.advance(6, TimeUnit.SECONDS);

    result = cachedSpecStorage.getFeatureSpecs(Collections.singletonList("feature_1"));
    result1 = cachedSpecStorage.getStorageSpecs(Collections.singletonList("storage_1"));
    result2 = cachedSpecStorage.getEntitySpecs(Collections.singletonList("entity_1"));
    assertThat(result.size(), equalTo(1));
    assertThat(result1.size(), equalTo(1));
    assertThat(result2.size(), equalTo(1));
  }
}