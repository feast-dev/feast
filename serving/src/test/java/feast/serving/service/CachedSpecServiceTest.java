/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
 */
package feast.serving.service;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

public class CachedSpecServiceTest {

  private File configFile;
  private Store store;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock CoreSpecService coreService;

  private Map<String, FeatureSetSpec> featureSetSpecs;
  private CachedSpecService cachedSpecService;

  @Before
  public void setUp() throws IOException {
    initMocks(this);

    configFile = File.createTempFile("serving", ".yml");
    String yamlString =
        "name: SERVING\n"
            + "type: REDIS\n"
            + "redis_config:\n"
            + "  host: localhost\n"
            + "  port: 6379\n"
            + "subscriptions:\n"
            + "- name: fs1\n"
            + "  version: \">0\"\n"
            + "- name: fs2\n"
            + "  version: \">0\"";
    BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(yamlString);
    writer.close();

    store =
        Store.newBuilder()
            .setName("SERVING")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
            .addSubscriptions(Subscription.newBuilder().setName("fs1").setVersion(">0").build())
            .addSubscriptions(Subscription.newBuilder().setName("fs2").setVersion(">0").build())
            .build();

    when(coreService.updateStore(UpdateStoreRequest.newBuilder().setStore(store).build()))
        .thenReturn(UpdateStoreResponse.newBuilder().setStore(store).build());

    featureSetSpecs = new LinkedHashMap<>();
    featureSetSpecs.put("fs1:1", FeatureSetSpec.newBuilder().setName("fs1").setVersion(1).build());
    featureSetSpecs.put("fs1:2", FeatureSetSpec.newBuilder().setName("fs1").setVersion(2).build());
    featureSetSpecs.put("fs2:1", FeatureSetSpec.newBuilder().setName("fs2").setVersion(1).build());

    List<FeatureSetSpec> fs1FeatureSets =
        Lists.newArrayList(featureSetSpecs.get("fs1:1"), featureSetSpecs.get("fs1:2"));
    List<FeatureSetSpec> fs2FeatureSets = Lists.newArrayList(featureSetSpecs.get("fs2:1"));
    when(coreService.listFeatureSets(
            ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    ListFeatureSetsRequest.Filter.newBuilder()
                        .setFeatureSetName("fs1")
                        .setFeatureSetVersion(">0")
                        .build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addAllFeatureSets(fs1FeatureSets).build());
    when(coreService.listFeatureSets(
            ListFeatureSetsRequest.newBuilder()
                .setFilter(
                    ListFeatureSetsRequest.Filter.newBuilder()
                        .setFeatureSetName("fs2")
                        .setFeatureSetVersion(">0")
                        .build())
                .build()))
        .thenReturn(ListFeatureSetsResponse.newBuilder().addAllFeatureSets(fs2FeatureSets).build());

    cachedSpecService = new CachedSpecService(coreService, configFile.toPath());
  }

  @After
  public void tearDown() {
    configFile.delete();
  }

  @Test
  public void shouldPopulateAndReturnStore() {
    cachedSpecService.populateCache();
    Store actual = cachedSpecService.getStore();
    assertThat(actual, equalTo(store));
  }

  @Test
  public void shouldPopulateAndReturnFeatureSets() {
    cachedSpecService.populateCache();
    assertThat(cachedSpecService.getFeatureSet("fs1", 1), equalTo(featureSetSpecs.get("fs1:1")));
    assertThat(cachedSpecService.getFeatureSet("fs1", 2), equalTo(featureSetSpecs.get("fs1:2")));
    assertThat(cachedSpecService.getFeatureSet("fs2", 1), equalTo(featureSetSpecs.get("fs2:1")));
  }
}
