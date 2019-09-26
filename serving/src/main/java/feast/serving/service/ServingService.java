package feast.serving.service;

import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionRequest;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;

public interface ServingService {
  GetFeastServingTypeResponse GetFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest);

  GetOnlineFeaturesResponse GetOnlineFeatures(GetFeaturesRequest getFeaturesRequest);

  GetBatchFeaturesResponse GetBatchFeatures(GetFeaturesRequest getFeaturesRequest);

  GetBatchFeaturesFromCompletedJobResponse GetBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest getBatchFeaturesFromCompletedJobRequest);

  GetStagingLocationResponse GetStagingLocation(
      GetStagingLocationRequest getStagingLocationRequest);

  LoadBatchFeaturesResponse LoadBatchFeatures(LoadBatchFeaturesRequest loadBatchFeaturesRequest);

  ReloadJobStatusResponse ReloadJobStatus(ReloadJobStatusRequest reloadJobStatusRequest);
}
