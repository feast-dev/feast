package feast.serving.service;

import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobRequest;
import feast.serving.ServingAPIProto.ReloadJobResponse;

public interface ServingService {
  GetFeastServingTypeResponse getFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest);

  GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest getFeaturesRequest);

  GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest getFeaturesRequest);

  GetStagingLocationResponse getStagingLocation(
      GetStagingLocationRequest getStagingLocationRequest);

  LoadBatchFeaturesResponse loadBatchFeatures(LoadBatchFeaturesRequest loadBatchFeaturesRequest);

  ReloadJobResponse reloadJob(ReloadJobRequest reloadJobStatusRequest);
}
