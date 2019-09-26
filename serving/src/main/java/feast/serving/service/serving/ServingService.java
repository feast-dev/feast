package feast.serving.service.serving;

import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;

public interface ServingService {
  // Get Feast serving storage type (online or batch)
  GetFeastServingTypeResponse getFeastServingType();

  // Get online features from Feast serving. This is a synchronous response.
  GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest request);

  // Get batch features from Feast serving. This is an async job.
  GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest request);

  // Get the URI(s) to download batch feature values from a successful download job.
  GetBatchFeaturesFromCompletedJobResponse getBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest request);

  // Get the URI prefix where the client can upload files to be accessed by Feast serving.
  GetStagingLocationResponse getStagingLocation(GetStagingLocationRequest request);

  // Load batch features from a list of source URIs asynchronously. The source
  // URIs must represent a feature set of a specific version.
  LoadBatchFeaturesResponse loadBatchFeatures(LoadBatchFeaturesRequest request);

  // Reload the job status with the latest state.
  ReloadJobStatusResponse reloadJobStatus(ReloadJobStatusRequest request);
}
