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

package feast.ingestion.service;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.Test;

public class FileSpecServiceTest {

  static Path getLocalCoreSpecsPath() {
    return Paths.get(Resources.getResource("core_specs/").getPath());
  }

  public FileSpecService getFileSpecService() {
    return new FileSpecService(getLocalCoreSpecsPath().toString());
  }

  @Test
  public void testGetSpecs() {
    FileSpecService service = getFileSpecService();

    List<String> storageIds = Lists.newArrayList("TEST_WAREHOUSE", "TEST_ERRORS", "TEST_SERVING");
    List<String> entities = Lists.newArrayList("testEntity");
    List<String> featureIds = Lists
        .newArrayList("testEntity.hour.redisString", "testEntity.none.testString",
            "testEntity.none.redisString", "testEntity.day.testInt64", "testEntity.none.redisInt32",
            "testEntity.none.testInt32", "testEntity.none.testInt64");
    assertThat(service.getEntitySpecs(entities).keySet(), containsInAnyOrder(entities.toArray()));
    assertThat(service.getFeatureSpecs(featureIds).keySet(), containsInAnyOrder(featureIds.toArray()));
    assertThat(service.getStorageSpecs(storageIds).keySet(), containsInAnyOrder(storageIds.toArray()));
  }
}
