package feast.serving.service.serving;

import com.google.cloud.bigquery.BigQuery;
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

public class BigQueryServingService implements ServingService {
  private BigQuery bigquery;

  public BigQueryServingService(BigQuery bigquery) {
    this.bigquery = bigquery;
  }

  @Override
  public GetFeastServingTypeResponse getFeastServingType() {
    return null;
  }

  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest request) {
    throw new UnsupportedOperationException(
        "BigQueryFeastServing only supports offline features retrieval");
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest request) {

    return null;
  }

  @Override
  public GetBatchFeaturesFromCompletedJobResponse getBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest request) {
    return null;
  }

  @Override
  public GetStagingLocationResponse getStagingLocation(GetStagingLocationRequest request) {
    return null;
  }

  @Override
  public LoadBatchFeaturesResponse loadBatchFeatures(LoadBatchFeaturesRequest request) {
    return null;
  }

  @Override
  public ReloadJobStatusResponse reloadJobStatus(ReloadJobStatusRequest request) {
    return null;
  }

}
