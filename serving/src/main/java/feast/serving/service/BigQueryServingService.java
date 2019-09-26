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

public class BigQueryServingService implements ServingService {
  @Override
  public GetFeastServingTypeResponse GetFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest) {
    return null;
  }

  @Override
  public GetOnlineFeaturesResponse GetOnlineFeatures(GetFeaturesRequest getFeaturesRequest) {
    return null;
  }

  @Override
  public GetBatchFeaturesResponse GetBatchFeatures(GetFeaturesRequest getFeaturesRequest) {
    return null;
  }

  @Override
  public GetBatchFeaturesFromCompletedJobResponse GetBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest getBatchFeaturesFromCompletedJobRequest) {
    return null;
  }

  @Override
  public GetStagingLocationResponse GetStagingLocation(
      GetStagingLocationRequest getStagingLocationRequest) {
    return null;
  }

  @Override
  public LoadBatchFeaturesResponse LoadBatchFeatures(
      LoadBatchFeaturesRequest loadBatchFeaturesRequest) {
    return null;
  }

  @Override
  public ReloadJobStatusResponse ReloadJobStatus(ReloadJobStatusRequest reloadJobStatusRequest) {
    return null;
  }
}
