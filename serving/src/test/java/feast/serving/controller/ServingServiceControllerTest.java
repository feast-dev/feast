package feast.serving.controller;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.timgroup.statsd.StatsDClient;
import feast.serving.FeastProperties;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDataset;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
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

public class ServingServiceControllerTest {
  private static final String FEATURE_SET_NAME = "feature_set_1";
  private static final int FEATURE_SET_VER = 1;
  private static final String FN_REGION = "region";
  private static final String FN_DRIVER_ID = "driver_id";
  private static final String FN_FEATURE_1 = "feature_1";

  private static final String FN_REGION_VAL = "id";
  private static final String FN_DRIVER_ID_VAL = "100";

  @Mock
  private ServingService mockServingService;

  @Mock
  private StreamObserver<GetOnlineFeaturesResponse> mockStreamObserver;

  private GetFeaturesRequest validRequest;

  private ServingServiceController service;

  @Before
  public void setUp() {
    initMocks(this);

    EntityDataset entityDataSet = getEntityDatasetBuilder(getEntityDatasetRowBuilder()).build();
    validRequest = GetFeaturesRequest.newBuilder().addFeatureSets(getFeatureSet())
        .setEntityDataset(entityDataSet).build();

    Tracer tracer = Configuration.fromEnv("dummy").getTracer();
    FeastProperties feastProperties = new FeastProperties();
    service = new ServingServiceController(mockServingService, feastProperties, tracer);
  }

  @Test
  public void shouldPassValidRequestAsIs() {
    service.getOnlineFeatures(validRequest, mockStreamObserver);
    Mockito.verify(mockServingService).getOnlineFeatures(validRequest);
  }

  @Test
  public void shouldCallOnErrorIfEntityDatasetIsNotSet() {
    GetFeaturesRequest missingEntityName =
        GetFeaturesRequest.newBuilder(validRequest).clearEntityDataset().build();
    service.getOnlineFeatures(missingEntityName, mockStreamObserver);
    Mockito.verify(mockStreamObserver).onError(Mockito.any(StatusRuntimeException.class));
  }

  @Test
  public void shouldCallOnErrorIfEntityDatasetRowAndFieldNameSizeMismatch() {
    // Adding an additional feature value
    EntityDataset sizeMismatchEntityDataset = EntityDataset
        .newBuilder(validRequest.getEntityDataset()).addEntityNames("some_random_field_name")
        .build();
    GetFeaturesRequest sizeMismatch = GetFeaturesRequest.newBuilder(validRequest)
        .setEntityDataset(sizeMismatchEntityDataset).build();
    service.getOnlineFeatures(sizeMismatch, mockStreamObserver);
    Mockito.verify(mockStreamObserver).onError(Mockito.any(StatusRuntimeException.class));
  }

  private FeatureSet getFeatureSet() {
    return FeatureSet.newBuilder().setName(FEATURE_SET_NAME)
        .setVersion(FEATURE_SET_VER).addFeatureNames(FN_FEATURE_1).build();
  }

  private EntityDataset.Builder getEntityDatasetBuilder(
      EntityDatasetRow.Builder entityDataSetRowBuilder) {
    return EntityDataset.newBuilder()
        .addEntityNames(FN_REGION)
        .addEntityNames(FN_DRIVER_ID)
        .addEntityDatasetRows(entityDataSetRowBuilder);
  }

  private EntityDatasetRow.Builder getEntityDatasetRowBuilder() {
    return EntityDatasetRow.newBuilder()
        .addEntityIds(Value.newBuilder().setStringVal(FN_REGION_VAL))
        .addEntityIds(Value.newBuilder().setStringVal(FN_DRIVER_ID_VAL));
  }
}