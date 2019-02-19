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

package feast.serving.grpc;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import feast.serving.ServingAPIProto.QueryFeaturesRequest;
import feast.serving.ServingAPIProto.QueryFeaturesResponse;
import feast.serving.service.FeastServing;
import feast.serving.service.FeatureRetrievalDispatcher;
import feast.serving.service.FeatureStorageRegistry;
import feast.serving.service.SpecStorage;
import feast.serving.testutil.FakeSpecStorage;
import feast.specs.FeatureSpecProto.FeatureSpec;
import io.opentracing.util.GlobalTracer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FeastServingTest {

  SpecStorage specStorage;

  @Mock FeatureStorageRegistry featureStorageRegistry;
  @Mock FeatureRetrievalDispatcher featureRetrievalDispatcher;

  // class under test
  private FeastServing feast;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    specStorage = new FakeSpecStorage();
    feast =
        new FeastServing(
            featureRetrievalDispatcher, featureStorageRegistry, specStorage, GlobalTracer.get());
  }

  @Test
  public void shouldReturnSameEntityNameAsRequest() {
    String entityName = "driver";
    QueryFeaturesRequest request =
        QueryFeaturesRequest.newBuilder()
            .setEntityName(entityName)
            .addFeatureId("driver.day.total_completed_booking")
            .build();

    QueryFeaturesResponse response = feast.queryFeatures(request);

    assertNotNull(response);
    assertThat(response.getEntityName(), equalTo(entityName));
  }

  @Test
  public void shouldPassValidRequestToFeatureRetrievalDispatcher() {
    String entityName = "driver";
    Collection<String> entityIds = Arrays.asList("entity1", "entity2", "entity3");
    Collection<String> featureIds = Arrays.asList("driver.day.total_completed_booking");
    QueryFeaturesRequest request =
        QueryFeaturesRequest.newBuilder()
            .setEntityName(entityName)
            .addAllEntityId(entityIds)
            .addAllFeatureId(featureIds)
            .build();

    ArgumentCaptor<String> entityNameArg = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<List<String>> entityIdsArg = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<Collection<FeatureSpec>> featureSpecArg =
        ArgumentCaptor.forClass(Collection.class);

    QueryFeaturesResponse response = feast.queryFeatures(request);
    verify(featureRetrievalDispatcher)
        .dispatchFeatureRetrieval(
            entityNameArg.capture(), entityIdsArg.capture(), featureSpecArg.capture());

    assertNotNull(response);
    assertThat(response.getEntityName(), equalTo(entityName));
    assertThat(entityNameArg.getValue(), equalTo(entityName));
    assertThat(entityIdsArg.getValue(), containsInAnyOrder(entityIds.toArray()));
  }
}
