package feast.serving.service;

import feast.serving.testutil.FakeRedisCoreService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CachedSpecStorageTest {

  private static final String STORE_ID = "SERVING";

  @Mock
  CachedSpecStorage cachedSpecStorage;

  @Mock
  SpecStorage fakeCoreService;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    fakeCoreService = new FakeRedisCoreService();
  }

  @Test
  public void populateCache_shouldPassStoreFetched() {
    // Store details not fetched from CoreService
    Assert.assertNull(cachedSpecStorage.getStoreDetails(STORE_ID));
    final CachedSpecStorage cachedSpecStorage = new CachedSpecStorage(fakeCoreService, STORE_ID);
    cachedSpecStorage.populateCache();
    // Store details fetched from CoreService
    Assert.assertNotNull(cachedSpecStorage.getStoreDetails(STORE_ID));
  }
}
