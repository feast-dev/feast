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

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import feast.core.CoreServiceGrpc.CoreServiceImplBase;
import feast.core.CoreServiceProto.CoreServiceTypes.*;
import feast.serving.exception.SpecRetrievalException;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.types.GranularityProto.Granularity.Enum;
import feast.types.ValueProto.ValueType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertThat;

public class CoreServiceTest {

  @Rule public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  @Rule public final ExpectedException expectedException = ExpectedException.none();

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
        Arrays.asList("driver.day.total_accepted_booking", "driver.second.ping_location");
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
        Arrays.asList("driver.day.total_accepted_booking", "driver.second.ping_location");
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
        Arrays.asList("driver.day.total_accepted_booking", "driver.second.ping_location");
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
  public void getStorageSpecs_shouldSendCorrectRequest() {
    List<String> storageIds = Arrays.asList("redis1", "big_table1");
    AtomicReference<GetStorageRequest> deliveredRequest = new AtomicReference<>();
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void getStorage(
              GetStorageRequest request, StreamObserver<GetStorageResponse> responseObserver) {
            deliveredRequest.set(request);
            responseObserver.onNext(
                GetStorageResponse.newBuilder()
                    .addAllStorageSpecs(getFakeStorageSpecs().values())
                    .build());
            responseObserver.onCompleted();
          }
        };

    serviceRegistry.addService(service);

    client.getStorageSpecs(storageIds);

    List<ByteString> expected =
        storageIds.stream().map(s -> ByteString.copyFromUtf8(s)).collect(Collectors.toList());
    List<ByteString> actual = deliveredRequest.get().getIdsList().asByteStringList();

    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void getStorageSpecs_shouldReturnRequestedStorageSpecs() {
    List<String> storageIds = Arrays.asList("redis1", "big_table1");
    AtomicReference<GetStorageRequest> deliveredRequest = new AtomicReference<>();
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void getStorage(
              GetStorageRequest request, StreamObserver<GetStorageResponse> responseObserver) {
            deliveredRequest.set(request);
            responseObserver.onNext(
                GetStorageResponse.newBuilder()
                    .addAllStorageSpecs(getFakeStorageSpecs().values())
                    .build());
            responseObserver.onCompleted();
          }
        };

    serviceRegistry.addService(service);

    Map<String, StorageSpec> results = client.getStorageSpecs(storageIds);

    assertThat(results.entrySet(), everyItem(isIn(getFakeStorageSpecs().entrySet())));
  }

  @Test
  public void getStorageSpecs_shouldThrowSpecsRetrievalExceptionWhenErrorHappen() {
    expectedException.expect(SpecRetrievalException.class);
    expectedException.expectMessage("Unable to retrieve storage specs");
    expectedException.expectCause(instanceOf(StatusRuntimeException.class));

    List<String> storageIds = Arrays.asList("redis1", "big_table1");
    client.getStorageSpecs(storageIds);
  }

  @Test
  public void getAllStorageSpecs_shouldReturnRequestedStorageSpecs() {
    CoreServiceImplBase service =
        new CoreServiceImplBase() {
          @Override
          public void listStorage(
              Empty request, StreamObserver<ListStorageResponse> responseObserver) {
            responseObserver.onNext(
                ListStorageResponse.newBuilder()
                    .addAllStorageSpecs(getFakeStorageSpecs().values())
                    .build());
            responseObserver.onCompleted();
          }
        };

    serviceRegistry.addService(service);

    Map<String, StorageSpec> results = client.getAllStorageSpecs();

    assertThat(results.entrySet(), everyItem(isIn(getFakeStorageSpecs().entrySet())));
  }

  @Test
  public void getAllStorageSpecs_shouldThrowSpecsRetrievalExceptionWhenErrorHappen() {
    expectedException.expect(SpecRetrievalException.class);
    expectedException.expectMessage("Unable to retrieve storage specs");
    expectedException.expectCause(instanceOf(StatusRuntimeException.class));

    client.getAllStorageSpecs();
  }

  private Map<String, FeatureSpec> getFakeFeatureSpecs() {
    FeatureSpec spec1 =
        FeatureSpec.newBuilder()
            .setId("driver.day.total_accepted_booking")
            .setName("total_accepted_booking")
            .setOwner("dummy@go-jek.com")
            .setDescription("awesome feature")
            .setGranularity(Enum.DAY)
            .setValueType(ValueType.Enum.STRING)
            .build();

    FeatureSpec spec2 =
        FeatureSpec.newBuilder()
            .setId("driver.second.ping")
            .setName("ping")
            .setOwner("dummy@go-jek.com")
            .setDescription("awesome feature")
            .setGranularity(Enum.SECOND)
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

  private Map<String, StorageSpec> getFakeStorageSpecs() {
    StorageSpec spec1 = StorageSpec.newBuilder().setId("redis1").build();

    StorageSpec spec2 = StorageSpec.newBuilder().setId("bigtable1").build();

    return Stream.of(spec1, spec2)
        .collect(Collectors.toMap(StorageSpec::getId, Function.identity()));
  }
}
