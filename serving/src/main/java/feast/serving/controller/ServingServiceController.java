package feast.serving.controller;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.timgroup.statsd.StatsDClient;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionRequest;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.FeatureSet;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingServiceGrpc.ServingServiceImplBase;
import feast.serving.service.ServingService;
import feast.storage.RedisProto.RedisKey;
import feast.types.FeatureRowProto.FeatureRow;
import io.grpc.stub.StreamObserver;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
@GRpcService
public class ServingServiceController extends ServingServiceImplBase {
  private final ServingService servingService;
  private StatsDClient statsDClient;

  @Value("${feast.version}")
  private String feastVersion;

  @Autowired
  public ServingServiceController(ServingService servingService) {
    this.servingService = servingService;
  }

  @Autowired(required = false)
  public void setStatsDClient(StatsDClient statsDClient) {
    this.statsDClient = statsDClient;
  }

  @Override
  public void getFeastServingVersion(
      GetFeastServingVersionRequest request,
      StreamObserver<GetFeastServingVersionResponse> responseObserver) {
    responseObserver.onNext(
        GetFeastServingVersionResponse.newBuilder().setVersion(feastVersion).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getFeastServingType(
      GetFeastServingTypeRequest request,
      StreamObserver<GetFeastServingTypeResponse> responseObserver) {
    responseObserver.onNext(servingService.GetFeastServingType(request));
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineFeatures(GetFeaturesRequest request,
      StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
    try()


    // try (Scope scope = GlobalTracer.get().buildSpan("getOnlineFeatures").startActive(true)) {
    //   List<String> entityNames = request.getEntityDataset().getEntityNamesList();
    //   List<EntityDataSetRow> entityDataSetRows = request.get
    //       .getEntityDataSetRowsList();
    //   GetOnlineFeaturesResponse.Builder getOnlineFeatureResponseBuilder = GetOnlineFeaturesResponse
    //       .newBuilder();
    //
    //   // Create a list of keys to be fetched from Redis
    //   List<FeatureSet> featureSets = request.getFeatureSetsList();
    //   for (FeatureSet featureSet : featureSets) {
    //     List<RedisKey> redisKeys = getRedisKeys(entityNames, entityDataSetRows, featureSet);
    //
    //     // Convert ProtocolStringList to list of Strings
    //     List<String> requestedColumns = featureSet.getFeatureNamesList()
    //         .asByteStringList().stream()
    //         .map(ByteString::toStringUtf8)
    //         .collect(Collectors.toList());
    //
    //     List<FeatureRow> featureRows = new ArrayList<>();
    //     try {
    //       featureRows = sendAndProcessMultiGet(redisKeys, requestedColumns, featureSet);
    //     } catch (InvalidProtocolBufferException e) {
    //       log.error("Unable to parse protobuf", e);
    //       throw new FeatureRetrievalException("Unable to parse protobuf while retrieving feature",
    //           e);
    //     } finally {
    //       FeatureDataSet featureDataSet = FeatureDataSet.newBuilder().setName(featureSet.getName())
    //           .setVersion(featureSet.getVersion()).addAllFeatureRows(featureRows).build();
    //       getOnlineFeatureResponseBuilder.addFeatureDataSets(featureDataSet);
    //     }
    //   }
    //
    //   return getOnlineFeatureResponseBuilder.build();
    // }

    responseObserver.onNext(servingService.GetOnlineFeatures(request));
    responseObserver.onCompleted();
  }

}
