/*
 * Copyright 2018 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.serving.service;

import feast.serving.service.spec.CachedSpecService;
import feast.serving.service.spec.SpecService;
import feast.serving.testutil.FakeRedisCoreService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CachedSpecServiceTest {

  private static final String STORE_ID = "SERVING";

  @Mock
  CachedSpecService cachedSpecStorage;

  @Mock
  SpecService fakeCoreService;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    fakeCoreService = new FakeRedisCoreService();
  }

  @Test
  public void populateCache_shouldPassStoreFetched() {
    // Store details not fetched from CoreService
    Assert.assertNull(cachedSpecStorage.getStoreDetails(STORE_ID));
    final CachedSpecService cachedSpecStorage = new CachedSpecService(fakeCoreService, STORE_ID);
    cachedSpecStorage.populateCache();
    // Store details fetched from CoreService
    Assert.assertNotNull(cachedSpecStorage.getStoreDetails(STORE_ID));
  }
}
