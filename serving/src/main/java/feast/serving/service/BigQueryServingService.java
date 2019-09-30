package feast.serving.service;

import com.google.cloud.bigquery.BigQuery;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsRequest.Filter;
import feast.core.FeatureSetProto.FeatureSetSpec;
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
import feast.serving.util.BigQueryUtil;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;

public class BigQueryServingService implements ServingService {
  private final BigQuery bigquery;
  private final String projectId;
  private final String datasetId;
  private final SpecService specService;

  public BigQueryServingService(
      BigQuery bigquery, String projectId, String datasetId, SpecService specService) {
    this.bigquery = bigquery;
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.specService = specService;
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
    List<FeatureSetSpec> featureSetSpecs =
        getFeaturesRequest.getFeatureSetsList().stream()
            .map(
                featureSet ->
                    specService.getFeatureSets(
                        GetFeatureSetsRequest.newBuilder()
                            .setFilter(
                                Filter.newBuilder().setFeatureSetName(featureSet.getName()).build())
                            .build()))
            .map(response -> response.getFeatureSetsList().get(0))
            .collect(Collectors.toList());
    String query =
        BigQueryUtil.createQuery(
            getFeaturesRequest.getFeatureSetsList(),
            featureSetSpecs,
            getFeaturesRequest.getEntityDataset().getEntityNamesList(),
            getFeaturesRequest.getEntityDataset().getEntityDatasetRowsList(),
            datasetId);
    System.out.println(query);
    return GetBatchFeaturesResponse.getDefaultInstance();
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
