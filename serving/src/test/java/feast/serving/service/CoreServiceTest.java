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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import feast.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.core.CoreServiceProto.CoreServiceTypes.GetEntitiesRequest;
import feast.core.CoreServiceProto.CoreServiceTypes.GetEntitiesResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.GetFeaturesRequest;
import feast.core.CoreServiceProto.CoreServiceTypes.GetFeaturesResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.GetStorageRequest;
import feast.core.CoreServiceProto.CoreServiceTypes.GetStorageResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.ListEntitiesResponse;
import feast.core.CoreServiceProto.CoreServiceTypes.ListFeaturesResponse;
import feast.serving.exception.SpecRetrievalException;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.ValueProto.ValueType;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CoreServiceTest {

  @Rule
  public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private CoreService client;

  @Before
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();

    grpcCleanupRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .fallbackHandlerRegistry(serviceRegistry)
            .build()
            .start());

    ManagedChannelBuilder builder = InProcessChannelBuilder.forName(serverName).directExecutor();
    client = new CoreService(builder);
  }

  @After
  public void tearDown() throws Exception {
    client.shutdown();
  }

  @Test
  public void getEntitySpecs_shouldSendCorrectRequest() {
    List<String> entityIds = Arrays.asList("driver", "customer");
    final AtomicReference<GetEntitiesRequest> requestDelivered = new AtomicReference<>();

    CoreServiceImplBase getEntitiesSpecImpl =
        new CoreServiceImplBase() {
          @Override
          public void getEntities(
              GetEntitiesRequest request, StreamObserver<GetEntitiesResponse> responseObserver) {
            requestDelivered.set(request);
            responseObserver.onNext(GetEntitiesResponse.newBuilder().build());
            responseObserver.onCompleted();
          }
        };
    serviceRegistry.addService(getEntitiesSpecImpl);

    client.getEntitySpecs(entityIds);

    List<ByteString> actual = requestDelivered.get().getIdsList().asByteStringList();
    List<ByteString> expected =
        entityIds.stream().map(s -> ByteString.copyFromUtf8(s)).collect(Collectors.toList());

    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void getEntitySpecs_shouldReturnRequestedEntitySpecs() {
    List<String> entityIds = Arrays.asList("driver", "customer");

    final GetEntitiesResponse response =
        GetEntitiesResponse.newBuilder().addAllEntities(getFakeEntitySpecs().values()).build();

    CoreServiceImplBase getEntitiesSpecImpl =
        new CoreServiceImplBase() {
          @Override
          public void getEntities(
              GetEntitiesRequest request, StreamObserver<GetEntitiesResponse> responseObserver) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }
        };
    serviceRegistry.addService(getEntitiesSpecImpl);

    Map<String, EntitySpec> result = client.getEntitySpecs(entityIds);
    assertThat(result.entrySet(), everyItem(isIn(getFakeEntitySpecs().entrySet())));
  }

  @Test
  public void getEntitySpecs_shouldThrowSpecRetrievalExceptionWhenErrorHappens() {
    expectedException.expect(SpecRetrievalException.class);
    expectedException.expectMessage("Unable to retrieve entity spec");
    expectedException.expectCause(instanceOf(StatusRuntimeException.class));

    List<String> entityIds = Arrays.asList("driver", "customer");

    client.getEntitySpecs(entityIds);
  }

  @Test
  public void getAllEntitySpecs_shouldReturnAllSpecs() {
    CoreServiceImplBase serviceImpl =
        new CoreServiceImplBase() {
          @Override
          public void listEntities(
              Empty request, StreamObserver<ListEntitiesResponse> responseObserver) {
            responseObserver.onNext(
                ListEntitiesResponse.newBuilder()
                    .addAllEntities(getFakeEntitySpecs().values())
                    .build());
            responseObserver.onCompleted();
          }
        };
    serviceRegistry.addService(serviceImpl);

    Map<String, EntitySpec> result = client.getAllEntitySpecs();

    assertThat(result.entrySet(), everyItem(isIn(getFakeEntitySpecs().entrySet())));
  }

  @Test
  public void getAllEntitySpecs_shouldThrowExceptionWhenErrorHappens() {
    expectedException.expect(SpecRetrievalException.class);
    expectedException.expectMessage("Unable to retrieve entity spec");
    expectedException.expectCause(instanceOf(StatusRuntimeException.class));

    client.getAllEntitySpecs();
  }

  @Test
  public void getFeatureSpecs_shouldSendCorrectRequest() {
    List<String> featureIds =
        Arrays.asList("driver.total_accepted_booking", "driver.ping_location");
    AtomicReference<GetFeaturesRequest> deliveredRequest = new AtomicReference<>();
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void getFeatures(
              GetFeaturesRequest request, StreamObserver<GetFeaturesResponse> responseObserver) {
            deliveredRequest.set(request);
            responseObserver.onNext(GetFeaturesResponse.newBuilder().build());
            responseObserver.onCompleted();
          }
        };

    serviceRegistry.addService(service);

    client.getFeatureSpecs(featureIds);

    List<ByteString> expected =
        featureIds.stream().map(s -> ByteString.copyFromUtf8(s)).collect(Collectors.toList());
    List<ByteString> actual = deliveredRequest.get().getIdsList().asByteStringList();

    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void getFeatureSpecs_shouldReturnRequestedFeatureSpecs() {
    List<String> featureIds =
        Arrays.asList("driver.total_accepted_booking", "driver.ping_location");
    AtomicReference<GetFeaturesRequest> deliveredRequest = new AtomicReference<>();
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void getFeatures(
              GetFeaturesRequest request, StreamObserver<GetFeaturesResponse> responseObserver) {
            deliveredRequest.set(request);
            responseObserver.onNext(
                GetFeaturesResponse.newBuilder()
                    .addAllFeatures(getFakeFeatureSpecs().values())
                    .build());
            responseObserver.onCompleted();
          }
        };

    serviceRegistry.addService(service);

    Map<String, FeatureSpec> results = client.getFeatureSpecs(featureIds);

    assertThat(results.entrySet(), everyItem(isIn(getFakeFeatureSpecs().entrySet())));
  }

  @Test
  public void getFeatureSpecs_shouldThrowSpecsRetrievalExceptionWhenErrorHappen() {
    expectedException.expect(SpecRetrievalException.class);
    expectedException.expectMessage("Unable to retrieve feature specs");
    expectedException.expectCause(instanceOf(StatusRuntimeException.class));

    List<String> featureIds =
        Arrays.asList("driver.total_accepted_booking", "driver.ping_location");
    client.getFeatureSpecs(featureIds);
  }

  @Test
  public void getAllFeatureSpecs_shouldReturnAllSpecs() {
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void listFeatures(
              Empty request, StreamObserver<ListFeaturesResponse> responseObserver) {
            responseObserver.onNext(
                ListFeaturesResponse.newBuilder()
                    .addAllFeatures(getFakeFeatureSpecs().values())
                    .build());
            responseObserver.onCompleted();
          }
        };
    serviceRegistry.addService(service);
    Map<String, FeatureSpec> results = client.getAllFeatureSpecs();

    assertThat(results.entrySet(), everyItem(isIn(getFakeFeatureSpecs().entrySet())));
  }

  @Test
  public void getAllFeatureSpecs_shouldThrowSpecRetrievalExceptionWhenErrorHappen() {
    expectedException.expect(SpecRetrievalException.class);
    expectedException.expectMessage("Unable to retrieve feature specs");
    expectedException.expectCause(instanceOf(StatusRuntimeException.class));

    client.getAllFeatureSpecs();
  }

  @Test
  public void getServingStorageSpec_shouldSendCorrectRequest() {
    AtomicReference<GetStorageRequest> deliveredRequest = new AtomicReference<>();
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void getStorage(
              GetStorageRequest request, StreamObserver<GetStorageResponse> responseObserver) {
            deliveredRequest.set(request);
            responseObserver.onNext(
                GetStorageResponse.newBuilder()
                    .addStorageSpecs(getFakeStorageSpec())
                    .build());
            responseObserver.onCompleted();
          }
        };
    serviceRegistry.addService(service);
    client.getServingStorageSpec();

    List<String> expected = Lists.newArrayList(FeastServing.SERVING_STORAGE_ID);
    List<String> actual = deliveredRequest.get().getIdsList();
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void getServingStorageSpec_shouldReturnRequestedStorageSpec() {
    AtomicReference<GetStorageRequest> deliveredRequest = new AtomicReference<>();
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void getStorage(
              GetStorageRequest request, StreamObserver<GetStorageResponse> responseObserver) {
            deliveredRequest.set(request);
            responseObserver.onNext(
                GetStorageResponse.newBuilder()
                    .addStorageSpecs(getFakeStorageSpec())
                    .build());
            responseObserver.onCompleted();
          }
        };

    serviceRegistry.addService(service);

    StorageSpec result = client.getServingStorageSpec();

    assertThat(result, equalTo(getFakeStorageSpec()));
  }

  @Test
  public void getStorageSpecs_shouldThrowSpecsRetrievalExceptionWhenErrorHappen() {
    expectedException.expect(SpecRetrievalException.class);
    expectedException.expectMessage("Unable to retrieve storage spec");
    expectedException.expectCause(instanceOf(StatusRuntimeException.class));
    client.getServingStorageSpec();
  }

  private Map<String, FeatureSpec> getFakeFeatureSpecs() {
    FeatureSpec spec1 =
        FeatureSpec.newBuilder()
            .setId("driver.total_accepted_booking")
            .setName("total_accepted_booking")
            .setOwner("dummy@go-jek.com")
            .setDescription("awesome feature")
            .setValueType(ValueType.Enum.STRING)
            .build();

    FeatureSpec spec2 =
        FeatureSpec.newBuilder()
            .setId("driver.ping")
            .setName("ping")
            .setOwner("dummy@go-jek.com")
            .setDescription("awesome feature")
            .setValueType(ValueType.Enum.INT64)
            .build();

    return Stream.of(spec1, spec2)
        .collect(Collectors.toMap(FeatureSpec::getId, Function.identity()));
  }

  private Map<String, EntitySpec> getFakeEntitySpecs() {
    EntitySpec spec1 =
        EntitySpec.newBuilder()
            .setName("driver")
            .setDescription("fake driver entity")
            .addTags("tag1")
            .addTags("tag2")
            .build();

    EntitySpec spec2 =
        EntitySpec.newBuilder()
            .setName("customer")
            .setDescription("fake customer entity")
            .addTags("tag1")
            .addTags("tag2")
            .build();

    return Stream.of(spec1, spec2)
        .collect(Collectors.toMap(EntitySpec::getName, Function.identity()));
  }

  private StorageSpec getFakeStorageSpec() {
    StorageSpec spec = StorageSpec.newBuilder().setId(FeastServing.SERVING_STORAGE_ID)
        .setType("redis")
        .putOptions("host", "localhost")
        .putOptions("port", "1234").build();
    return spec;
  }
}
