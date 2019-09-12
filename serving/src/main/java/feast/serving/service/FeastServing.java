package feast.serving.service;

import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetStatusResponse;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.GetUploadUrlResponse;
import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteRequest;
import feast.serving.ServingAPIProto.BatchFeaturesJob.SetUploadCompleteResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeastServingVersionResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;


public interface FeastServing {

  // Retrieve version information about this Feast deployment
  GetFeastServingVersionResponse getFeastServingVersion();

  // Get Feast serving storage type (online or batch)
  GetFeastServingTypeResponse getFeastServingType();

  // Get online features from Feast serving. This is a synchronous response.
  GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest request);

  // Get batch features from Feast serving. This is an async job.
  GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest request);

  // Get the current status of a batch feature request job
  GetStatusResponse getBatchFeaturesJobStatus(GetStatusRequest request);

  // Request a signed URL where a Feast client can upload user entity data
  GetUploadUrlResponse getBatchFeaturesJobUploadUrl(GetUploadUrlRequest request);

  // Set the state of the batch feature job to complete after user entity data has been uploaded
  SetUploadCompleteResponse setBatchFeaturesJobUploadComplete(SetUploadCompleteRequest request);
}
