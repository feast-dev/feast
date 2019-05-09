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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import feast.serving.config.AppConfig;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FeatureRetrievalDispatcherTest {

  @Mock
  FeatureStorageRegistry featureStorageRegistry;
  private FeatureRetrievalDispatcher dispatcher;
  private List<String> entityIds;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    entityIds = createEntityIds(10);

    dispatcher =
        new FeatureRetrievalDispatcher(
            featureStorageRegistry, GlobalTracer.get());
  }

  @Test
  public void shouldGetFeaturesFromStorage() {
    String entityName = "entity";
    FeatureStorage featureStorage = mock(FeatureStorage.class);
    when(featureStorage.getFeature(any(String.class), any(List.class), any(List.class)))
        .thenReturn(Collections.emptyList());
    when(featureStorageRegistry.get(any(String.class))).thenReturn(featureStorage);

    String featureId = "entity.feature_1";
    FeatureSpec featureSpec = FeatureSpec.newBuilder().setId(featureId).build();
    dispatcher.dispatchFeatureRetrieval(
        entityName, entityIds, Collections.singletonList(featureSpec));

    verify(featureStorage)
        .getFeature(entityName, entityIds, Collections.singletonList(featureSpec));
  }

  private List<String> createEntityIds(int count) {
    List<String> entityIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      entityIds.add("entity_" + i);
    }
    return entityIds;
  }
}
