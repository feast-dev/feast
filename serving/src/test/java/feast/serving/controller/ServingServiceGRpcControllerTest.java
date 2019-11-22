package feast.serving.controller;

import static org.mockito.MockitoAnnotations.initMocks;

import com.google.common.collect.Lists;
import com.google.protobuf.Timestamp;
import feast.serving.FeastProperties;
import feast.serving.ServingAPIProto.FeatureSetRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest.EntityRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.service.ServingService;
import feast.types.ValueProto.Value;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ServingServiceGRpcControllerTest {

  @Mock private ServingService mockServingService;

  @Mock private StreamObserver<GetOnlineFeaturesResponse> mockStreamObserver;

  private GetOnlineFeaturesRequest validRequest;

  private ServingServiceGRpcController service;

  @Before
  public void setUp() {
    initMocks(this);

    validRequest =
        GetOnlineFeaturesRequest.newBuilder()
            .addFeatureSets(
                FeatureSetRequest.newBuilder()
                    .setName("featureSet")
                    .setVersion(1)
                    .addAllFeatureNames(Lists.newArrayList("feature1", "feature2"))
                    .build())
            .addEntityRows(
                EntityRow.newBuilder()
                    .setEntityTimestamp(Timestamp.newBuilder().setSeconds(100))
                    .putFields("entity1", Value.newBuilder().setInt64Val(1).build())
                    .putFields("entity2", Value.newBuilder().setInt64Val(1).build()))
            .build();

    Tracer tracer = Configuration.fromEnv("dummy").getTracer();
    FeastProperties feastProperties = new FeastProperties();
    service = new ServingServiceGRpcController(mockServingService, feastProperties, tracer);
  }

  @Test
  public void shouldPassValidRequestAsIs() {
    service.getOnlineFeatures(validRequest, mockStreamObserver);
    Mockito.verify(mockServingService).getOnlineFeatures(validRequest);
  }

  @Test
  public void shouldCallOnErrorIfEntityDatasetIsNotSet() {
    GetOnlineFeaturesRequest missingEntityName =
        GetOnlineFeaturesRequest.newBuilder(validRequest).clearEntityRows().build();
    service.getOnlineFeatures(missingEntityName, mockStreamObserver);
    Mockito.verify(mockStreamObserver).onError(Mockito.any(StatusRuntimeException.class));
  }
}
