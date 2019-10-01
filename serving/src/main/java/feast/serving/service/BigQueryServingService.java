package feast.serving.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.storage.Storage;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsRequest.Filter;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.GetStagingLocationRequest;
import feast.serving.ServingAPIProto.GetStagingLocationResponse;
import feast.serving.ServingAPIProto.JobStatus;
import feast.serving.ServingAPIProto.JobType;
import feast.serving.ServingAPIProto.LoadBatchFeaturesRequest;
import feast.serving.ServingAPIProto.LoadBatchFeaturesResponse;
import feast.serving.ServingAPIProto.ReloadJobStatusRequest;
import feast.serving.ServingAPIProto.ReloadJobStatusResponse;
import feast.serving.util.BigQueryUtil;
import io.grpc.Status;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigQueryServingService implements ServingService {
  private final BigQuery bigquery;
  private final String projectId;
  private final String datasetId;
  private final SpecService specService;
  private final JobService jobService;
  private final String jobStagingLocation;
  private final Storage storage;

  public BigQueryServingService(
      BigQuery bigquery,
      String projectId,
      String datasetId,
      SpecService specService,
      JobService jobService,
      String jobStagingLocation,
      Storage storage) {
    this.bigquery = bigquery;
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.specService = specService;
    this.jobService = jobService;
    this.jobStagingLocation = jobStagingLocation;
    this.storage = storage;
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
            .filter(response -> response.getFeatureSetsList().size() > 1)
            .map(response -> response.getFeatureSetsList().get(0))
            .collect(Collectors.toList());

    if (getFeaturesRequest.getFeatureSetsList().size() != featureSetSpecs.size()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Some of the feature sets requested do not exist in Feast. Please check your request payload.")
          .asRuntimeException();
    }

    final String query =
        BigQueryUtil.createQuery(
            getFeaturesRequest.getFeatureSetsList(),
            featureSetSpecs,
            getFeaturesRequest.getEntityDataset().getEntityNamesList(),
            getFeaturesRequest.getEntityDataset().getEntityDatasetRowsList(),
            datasetId);
    log.debug("Running BigQuery query: {}", query);

    String feastJobId = UUID.randomUUID().toString();
    ServingAPIProto.Job feastJob =
        ServingAPIProto.Job.newBuilder()
            .setId(feastJobId)
            .setType(JobType.JOB_TYPE_DOWNLOAD)
            .setStatus(JobStatus.JOB_STATUS_PENDING)
            .build();
    jobService.upsert(feastJob);

    new Thread(
            () -> {
              QueryJobConfiguration queryConfig =
                  QueryJobConfiguration.newBuilder(query)
                      .setDefaultDataset(DatasetId.of(projectId, datasetId))
                      .build();
              Job queryJob = bigquery.create(JobInfo.of(queryConfig));
              jobService.upsert(
                  ServingAPIProto.Job.newBuilder()
                      .setId(feastJobId)
                      .setType(JobType.JOB_TYPE_DOWNLOAD)
                      .setStatus(JobStatus.JOB_STATUS_RUNNING)
                      .build());
              try {
                queryJob.waitFor();
              } catch (BigQueryException | InterruptedException e) {
                jobService.upsert(
                    ServingAPIProto.Job.newBuilder()
                        .setId(feastJobId)
                        .setType(JobType.JOB_TYPE_DOWNLOAD)
                        .setStatus(JobStatus.JOB_STATUS_DONE)
                        .setError(e.getMessage())
                        .build());
                return;
              }

              queryConfig = queryJob.getConfiguration();
              String exportTableDestinationUri =
                  String.format("%s/%s/*.avro", jobStagingLocation, feastJob);

              // Hardcode the format to Avro for now
              ExtractJobConfiguration extractConfig =
                  ExtractJobConfiguration.of(
                      queryConfig.getDestinationTable(), exportTableDestinationUri, "Avro");
              Job extractJob = bigquery.create(JobInfo.of(extractConfig));
              try {
                extractJob.waitFor();
              } catch (BigQueryException | InterruptedException e) {
                jobService.upsert(
                    ServingAPIProto.Job.newBuilder()
                        .setId(feastJobId)
                        .setType(JobType.JOB_TYPE_DOWNLOAD)
                        .setStatus(JobStatus.JOB_STATUS_DONE)
                        .setError(e.getMessage())
                        .build());
                return;
              }

              String bucket =
                  jobStagingLocation.substring(jobStagingLocation.indexOf("://") + 3).split("/")[0];
              String prefix = "";

              // Then list all files under that bucket, prefix
              // then return it

              // for (Blob blob : storage.l)
              //   jobService.upsert(
              //       ServingAPIProto.Job.newBuilder()
              //           .setId(feastJobId)
              //           .setType(JobType.JOB_TYPE_DOWNLOAD)
              //           .setStatus(JobStatus.JOB_STATUS_DONE)
              //           .build());
            })
        .start();

    return GetBatchFeaturesResponse.newBuilder().setJob(feastJob).build();
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
