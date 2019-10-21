package feast.serving.service;

import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetJobRequest;
import feast.serving.ServingAPIProto.GetJobResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;

public interface ServingService {
  GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest);

  GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest getFeaturesRequest);

  GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest);

  GetJobResponse getJob(GetJobRequest getJobRequest);
}
