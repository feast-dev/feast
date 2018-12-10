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

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import feast.serving.ServingAPIProto.QueryFeatures.Request;
import feast.serving.ServingAPIProto.QueryFeatures.Response;
import feast.serving.ServingAPIProto.RequestDetail;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.model.RequestDetailWithSpec;
import feast.serving.service.FeastServing;
import feast.serving.service.FeatureRetrievalDispatcher;
import feast.serving.service.FeatureStorageRegistry;
import feast.serving.service.SpecStorage;
import feast.serving.testutil.FakeSpecStorage;
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
    RequestDetail requestDetail =
        RequestDetail.newBuilder().setFeatureId("driver.day.total_completed_booking").build();
    TimestampRange tsRange =
        TimestampRange.newBuilder()
            .setStart(Timestamps.EPOCH)
            .setEnd(Timestamp.newBuilder().setSeconds(1234).build())
            .build();
    Request request =
        Request.newBuilder()
            .setEntityName(entityName)
            .addRequestDetails(requestDetail)
            .setTimestampRange(tsRange)
            .build();

    Response response = feast.queryFeatures(request);

    assertNotNull(response);
    assertThat(response.getEntityName(), equalTo(entityName));
  }

  @Test
  public void shouldPassValidRequestToFeatureRetrievalDispatcher() {
    String entityName = "driver";
    Collection<String> entityIds = Arrays.asList("entity1", "entity2", "entity3");
    RequestDetail req1 =
        RequestDetail.newBuilder().setFeatureId("driver.day.total_completed_booking").build();
    TimestampRange tsRange =
        TimestampRange.newBuilder()
            .setStart(Timestamps.EPOCH)
            .setEnd(Timestamp.newBuilder().setSeconds(1234).build())
            .build();
    Request request =
        Request.newBuilder()
            .setEntityName(entityName)
            .addAllEntityId(entityIds)
            .addRequestDetails(req1)
            .setTimestampRange(tsRange)
            .build();

    ArgumentCaptor<String> entityNameArg = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<List<String>> entityIdsArg = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<RequestDetailWithSpec>> requestsArg = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<TimestampRange> tsRangeArg = ArgumentCaptor.forClass(TimestampRange.class);

    Response response = feast.queryFeatures(request);
    verify(featureRetrievalDispatcher)
        .dispatchFeatureRetrieval(
            entityNameArg.capture(),
            entityIdsArg.capture(),
            requestsArg.capture(),
            tsRangeArg.capture());

    assertNotNull(response);
    assertThat(response.getEntityName(), equalTo(entityName));
    assertThat(entityNameArg.getValue(), equalTo(entityName));
    assertThat(entityIdsArg.getValue(), containsInAnyOrder(entityIds.toArray()));
    assertThat(tsRangeArg.getValue(), equalTo(tsRange));
  }
}
