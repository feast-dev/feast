package feast.serving.service;

import com.google.cloud.bigquery.BigQuery;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesFromCompletedJobResponse;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;
import io.grpc.Status;

public class BigQueryServingService implements ServingService {
  private final BigQuery bigquery;
  private final String projectId;
  private final String datasetId;

  public BigQueryServingService(BigQuery bigquery, String projectId, String datasetId) {
    this.bigquery = bigquery;
    this.projectId = projectId;
    this.datasetId = datasetId;
  }

  @Override
  public GetFeastServingTypeResponse getFeastServingType(
      GetFeastServingTypeRequest getFeastServingTypeRequest) {
    return GetFeastServingTypeResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_BATCH)
        .build();
  }

  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetFeaturesRequest getFeaturesRequest) {
    return null;
  }

  @Override
  public GetBatchFeaturesFromCompletedJobResponse getBatchFeaturesFromCompletedJob(
      GetBatchFeaturesFromCompletedJobRequest getBatchFeaturesFromCompletedJobRequest) {
    return null;
  }

  @Override
  public GetStagingLocationResponse getStagingLocation(
      GetStagingLocationRequest getStagingLocationRequest) {
    return null;
  }

  @Override
  public LoadBatchFeaturesResponse loadBatchFeatures(
      LoadBatchFeaturesRequest loadBatchFeaturesRequest) {
    return null;
  }

  @Override
  public ReloadJobStatusResponse reloadJobStatus(ReloadJobStatusRequest reloadJobStatusRequest) {
    return null;
  }
}
