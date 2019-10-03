package feast.serving.service;

import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetJobRequest;
import feast.serving.ServingAPIProto.GetJobResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;

public interface ServingService {
  GetFeastServingTypeResponse getFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest);

  GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest getFeaturesRequest);

  GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest getFeaturesRequest);

  GetJobResponse getJob(GetJobRequest getJobRequest);
}
