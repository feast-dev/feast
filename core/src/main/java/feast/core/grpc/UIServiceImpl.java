package feast.core.grpc;

import static io.grpc.Status.Code.INTERNAL;
import static io.grpc.Status.Code.INVALID_ARGUMENT;

import com.google.protobuf.Empty;
import feast.core.UIServiceGrpc.UIServiceImplBase;
import feast.core.UIServiceProto.UIServiceTypes.EntityDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureDetail;
import feast.core.UIServiceProto.UIServiceTypes.FeatureGroupDetail;
import feast.core.UIServiceProto.UIServiceTypes.GetEntityRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetEntityResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureGroupRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureGroupResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetFeatureResponse;
import feast.core.UIServiceProto.UIServiceTypes.GetStorageRequest;
import feast.core.UIServiceProto.UIServiceTypes.GetStorageResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListEntitiesResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListFeatureGroupsResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListFeaturesResponse;
import feast.core.UIServiceProto.UIServiceTypes.ListStorageResponse;
import feast.core.UIServiceProto.UIServiceTypes.StorageDetail;
import feast.core.model.EntityInfo;
import feast.core.model.FeatureGroupInfo;
import feast.core.model.FeatureInfo;
import feast.core.model.StorageInfo;
import feast.core.service.SpecService;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * GRPC Service exposing detailed information of feast's resources.
 */
@Slf4j
@GRpcService
public class UIServiceImpl extends UIServiceImplBase {

  private final SpecService specService;

  @Autowired
  public UIServiceImpl(SpecService specService) {
    this.specService = specService;
  }

  @Override
  public void getEntity(GetEntityRequest request,
      StreamObserver<GetEntityResponse> responseObserver) {
    String entityName = request.getId();

    try {
      List<EntityInfo> entityInfos = specService.getEntities(Collections.singletonList(entityName));
      EntityDetail entityDetail = entityInfos.get(0)
          .getEntityDetail();

      GetEntityResponse response = GetEntityResponse.newBuilder()
          .setEntity(entityDetail)
          .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      String errMsg = "Invalid entity name: " + entityName;
      log.error(errMsg, e);
      onError(responseObserver, INVALID_ARGUMENT, errMsg, e);
    } catch (Exception e) {
      String errMsg = "Error while retrieving entity with name: " + entityName;
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  @Override
  public void listEntities(Empty request, StreamObserver<ListEntitiesResponse> responseObserver) {
    try {
      List<EntityInfo> entityInfos = specService.listEntities();
      List<EntityDetail> entityDetails = entityInfos.stream()
          .map(EntityInfo::getEntityDetail)
          .collect(Collectors.toList());
      ListEntitiesResponse response = ListEntitiesResponse.newBuilder()
          .addAllEntities(entityDetails)
          .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String errMsg = "Error while getting all entities";
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  @Override
  public void getFeature(GetFeatureRequest request,
      StreamObserver<GetFeatureResponse> responseObserver) {
    String featureId = request.getId();
    try {
      List<FeatureInfo> featureInfos = specService
          .getFeatures(Collections.singletonList(featureId));
      FeatureDetail featureDetail = featureInfos.get(0)
          .getFeatureDetail(specService.getStorageSpecs());

      GetFeatureResponse resp = GetFeatureResponse.newBuilder()
          .setFeature(featureDetail)
          .build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      String errMsg = "Invalid feature ID: " + featureId;
      log.error(errMsg);
      onError(responseObserver, INVALID_ARGUMENT, errMsg, e);
    } catch (Exception e) {
      String errMsg = "Error while retrieving feature with ID: " + featureId;
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  @Override
  public void listFeatures(Empty request, StreamObserver<ListFeaturesResponse> responseObserver) {
    try {
      List<FeatureDetail> featureDetails = specService.listFeatures()
          .stream()
          .map((fi) -> fi.getFeatureDetail(specService.getStorageSpecs()))
          .collect(Collectors.toList());

      ListFeaturesResponse resp = ListFeaturesResponse.newBuilder()
          .addAllFeatures(featureDetails)
          .build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String errMsg = "Error while getting all features";
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  @Override
  public void getFeatureGroup(GetFeatureGroupRequest request,
      StreamObserver<GetFeatureGroupResponse> responseObserver) {
    String featureGroupId = request.getId();
    try {
      List<FeatureGroupInfo> featureGroupInfos = specService
          .getFeatureGroups(Collections.singletonList(featureGroupId));

      GetFeatureGroupResponse resp = GetFeatureGroupResponse.newBuilder()
          .setFeatureGroup(featureGroupInfos.get(0).getFeatureGroupDetail())
          .build();

      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      String errMsg = "Invalid feature group ID: " + featureGroupId;
      log.error(errMsg);
      onError(responseObserver, INVALID_ARGUMENT, errMsg, e);
    } catch (Exception e) {
      String errMsg = "Error while getting feature group with ID: " + featureGroupId;
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  @Override
  public void listFeatureGroups(Empty request,
      StreamObserver<ListFeatureGroupsResponse> responseObserver) {
    try {
      List<FeatureGroupInfo> featureGroupInfos = specService.listFeatureGroups();
      List<FeatureGroupDetail> featureGroupDetails = featureGroupInfos.stream()
          .map(FeatureGroupInfo::getFeatureGroupDetail)
          .collect(Collectors.toList());

      ListFeatureGroupsResponse resp = ListFeatureGroupsResponse.newBuilder()
          .addAllFeatureGroups(featureGroupDetails)
          .build();
      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String errMsg = "Error while getting all feature groups";
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  @Override
  public void getStorage(GetStorageRequest request,
      StreamObserver<GetStorageResponse> responseObserver) {
    String storageId = request.getId();
    try {
      List<StorageInfo> storageInfos = specService.getStorage(Collections.singletonList(storageId));
      GetStorageResponse resp = GetStorageResponse.newBuilder()
          .setStorage(storageInfos.get(0).getStorageDetail())
          .build();

      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      String errMsg = "Invalid storage ID: " + storageId;
      log.error(errMsg, e);
      onError(responseObserver, INVALID_ARGUMENT, errMsg, e);
    } catch (Exception e) {
      String errMsg = "Error while retrieving storage detail with ID: " + storageId;
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  @Override
  public void listStorage(Empty request, StreamObserver<ListStorageResponse> responseObserver) {
    try {
      List<StorageInfo> storageInfos = specService.listStorage();
      List<StorageDetail> storageDetails = storageInfos.stream()
          .map(StorageInfo::getStorageDetail)
          .collect(Collectors.toList());

      ListStorageResponse resp = ListStorageResponse.newBuilder()
          .addAllStorage(storageDetails)
          .build();

      responseObserver.onNext(resp);
      responseObserver.onCompleted();
    } catch (Exception e) {
      String errMsg = "Error while getting all storage details";
      log.error(errMsg, e);
      onError(responseObserver, INTERNAL, errMsg, e);
    }
  }

  private void onError(StreamObserver<?> responseObserver, Code errCode, String message,
      Throwable cause) {
    responseObserver.onError(Status.fromCode(errCode)
        .withDescription(message)
        .withCause(cause)
        .asException());
  }
}
