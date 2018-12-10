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

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.timgroup.statsd.StatsDClient;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;
import feast.serving.ServingAPIProto.QueryFeatures.Request;
import feast.serving.ServingAPIProto.QueryFeatures.Response;
import feast.serving.ServingAPIProto.RequestDetail;
import feast.serving.ServingAPIProto.RequestType;
import feast.serving.ServingAPIProto.TimestampRange;
import feast.serving.service.FeastServing;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServingGrpcServiceTest {

  @Mock private FeastServing mockFeast;

  @Mock private StreamObserver<Response> mockStreamObserver;

  @Mock private Response mockResponse;

  @Mock private StatsDClient statsDClient;

  private Request validRequest;

  private ServingGrpcService service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    RequestDetail req1 =
        RequestDetail.newBuilder().setFeatureId("driver.day.completed_booking").build();

    RequestDetail req2 =
        RequestDetail.newBuilder()
            .setFeatureId("driver.none.last_opportunity")
            .setType(RequestType.LIST)
            .setLimit(5)
            .build();

    TimestampRange tsRange =
        TimestampRange.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(10).build())
            .setEnd(Timestamp.newBuilder().setSeconds(11).build())
            .build();

    validRequest =
        Request.newBuilder()
            .setEntityName("driver")
            .addAllEntityId(Arrays.asList("driver1", "driver2", "driver3"))
            .addAllRequestDetails(Arrays.asList(req1, req2))
            .setTimestampRange(tsRange)
            .build();

    Tracer tracer = Configuration.fromEnv("dummy").getTracer();
    service = new ServingGrpcService(mockFeast, tracer, statsDClient);
  }

  @Test
  public void shouldPassValidRequestAsIs() {
    service.queryFeatures(validRequest, mockStreamObserver);
    verify(mockFeast).queryFeatures(validRequest);
  }

  @Test
  public void shouldUseNowAsStartAndEndInTimestampRangeIfMissing() {
    Request missingTsRangeReq = Request.newBuilder(validRequest).clearTimestampRange().build();

    ArgumentCaptor<Request> argCaptor = ArgumentCaptor.forClass(Request.class);

    when(mockFeast.queryFeatures(argCaptor.capture())).thenReturn(mockResponse);
    service.queryFeatures(missingTsRangeReq, mockStreamObserver);

    Timestamp now = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();
    TimestampRange actualTsRange = argCaptor.getValue().getTimestampRange();
    assertThat(
        (double) actualTsRange.getStart().getSeconds(),
        closeTo((double) now.getSeconds(), (double) 1));
    assertThat(
        (double) actualTsRange.getEnd().getSeconds(),
        closeTo((double) now.getSeconds(), (double) 1));
    System.out.println(argCaptor.getValue());
  }

  @Test
  public void shouldUseEndIfStartTimestampRangeIsMissing() {
    Timestamp end = Timestamp.newBuilder().setSeconds(1234).build();
    Request missingStartTsRangeReq =
        Request.newBuilder(validRequest)
            .clearTimestampRange()
            .setTimestampRange(TimestampRange.newBuilder().setEnd(end))
            .build();

    ArgumentCaptor<Request> argCaptor = ArgumentCaptor.forClass(Request.class);

    when(mockFeast.queryFeatures(argCaptor.capture())).thenReturn(mockResponse);
    service.queryFeatures(missingStartTsRangeReq, mockStreamObserver);

    TimestampRange actualTsRange = argCaptor.getValue().getTimestampRange();
    assertThat(actualTsRange.getStart(), equalTo(end));
    System.out.println(argCaptor.getValue());
  }

  @Test
  public void shouldUseNowIfEndTimestampRangeIsMissing() {
    Timestamp start = Timestamp.newBuilder().setSeconds(1234).build();
    Request missingEndTsRangeReq =
        Request.newBuilder(validRequest)
            .clearTimestampRange()
            .setTimestampRange(TimestampRange.newBuilder().setStart(start))
            .build();

    ArgumentCaptor<Request> argCaptor = ArgumentCaptor.forClass(Request.class);

    when(mockFeast.queryFeatures(argCaptor.capture())).thenReturn(mockResponse);
    service.queryFeatures(missingEndTsRangeReq, mockStreamObserver);

    Timestamp now = Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build();
    TimestampRange actualTsRange = argCaptor.getValue().getTimestampRange();
    assertThat(
        (double) actualTsRange.getEnd().getSeconds(),
        closeTo((double) now.getSeconds(), (double) 1));
    System.out.println(argCaptor.getValue());
  }

  @Test
  public void shouldCallOnErrorIfEntityNameIsNotSet() {
    Request missingEntityName = Request.newBuilder(validRequest).clearEntityName().build();

    service.queryFeatures(missingEntityName, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfEntityIdsIsNotSet() {
    Request missingEntityIds = Request.newBuilder(validRequest).clearEntityId().build();

    service.queryFeatures(missingEntityIds, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfRequestDetailsIsNotSet() {
    Request missingRequestDetails = Request.newBuilder(validRequest).clearRequestDetails().build();

    service.queryFeatures(missingRequestDetails, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfRequestDetailsContainsDifferentEntity() {
    RequestDetail req3 = RequestDetail.newBuilder().setFeatureId("customer.day.order_made").build();
    Request differentEntityReq = Request.newBuilder(validRequest).addRequestDetails(req3).build();

    service.queryFeatures(differentEntityReq, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfTimestampRangeStartIsAfterEnd() {
    TimestampRange invalidTsRange =
        TimestampRange.newBuilder()
            .setStart(Timestamps.fromSeconds(1001))
            .setEnd(Timestamps.fromSeconds(1000))
            .build();

    Request invalidTsRangeReq =
        Request.newBuilder(validRequest).setTimestampRange(invalidTsRange).build();

    service.queryFeatures(invalidTsRangeReq, mockStreamObserver);

    verify(mockStreamObserver).onError(any(StatusRuntimeException.class));
  }
}
