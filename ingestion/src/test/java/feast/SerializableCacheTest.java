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

package feast;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class SerializableCacheTest {
  @Test
  public void testGet() {

    final AtomicInteger count = new AtomicInteger(0);
    SerializableCache<String, String> cache =
        SerializableCache.<String, String>builder()
            .loadingFunction((str) -> str + " " + count.incrementAndGet())
            .build();

    Assert.assertEquals("hello 1", cache.get("hello"));
    Assert.assertEquals("hello 1", cache.get("hello"));
  }

  @Test
  public void testGetMaxSize() {

    final AtomicInteger count = new AtomicInteger(0);
    SerializableCache<String, String> cache =
        SerializableCache.<String, String>builder()
            .loadingFunction((str) -> str + " " + count.incrementAndGet())
            .maximumSize(2)
            .build();

    Assert.assertEquals("x 1", cache.get("x"));
    Assert.assertEquals("y 2", cache.get("y"));

    // still in cache
    Assert.assertEquals("x 1", cache.get("x"));
    Assert.assertEquals("y 2", cache.get("y"));

    // z will evict x from cache
    Assert.assertEquals("z 3", cache.get("z"));

    // x not in cache and evict y
    Assert.assertEquals("x 4", cache.get("x"));
    Assert.assertEquals("y 5", cache.get("y"));
  }

  @Test
  public void testGetWithExpiry() throws InterruptedException {
    final AtomicInteger count = new AtomicInteger(0);
    SerializableCache<String, String> cache =
        SerializableCache.<String, String>builder()
            .loadingFunction((str) -> str + " " + count.incrementAndGet())
            .expireAfterAccess(Duration.ofMillis(100))
            .build();

    Assert.assertEquals("x 1", cache.get("x"));
    Assert.assertEquals("x 1", cache.get("x"));

    Thread.sleep(200);
    Assert.assertEquals("x 2", cache.get("x"));
  }
}
