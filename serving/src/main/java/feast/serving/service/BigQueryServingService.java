package feast.serving.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsRequest.Filter;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.DataFormat;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingTypeRequest;
import feast.serving.ServingAPIProto.GetFeastServingTypeResponse;
import feast.serving.ServingAPIProto.GetFeaturesRequest;
import feast.serving.ServingAPIProto.GetFeaturesRequest.EntityDatasetRow;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.JobStatus;
import feast.serving.ServingAPIProto.JobType;
import feast.serving.ServingAPIProto.ReloadJobRequest;
import feast.serving.ServingAPIProto.ReloadJobResponse;
import feast.serving.util.BigQueryUtil;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
    // TODO: Consider default maxAge in featureSetSpec during retrieval

    List<FeatureSetSpec> featureSetSpecs =
        getFeaturesRequest.getFeatureSetsList().stream()
            .map(
                featureSet ->
                    specService.getFeatureSets(
                        GetFeatureSetsRequest.newBuilder()
                            .setFilter(
                                Filter.newBuilder()
                                    .setFeatureSetName(featureSet.getName())
                                    .setFeatureSetVersion(String.valueOf(featureSet.getVersion()))
                                    .build())
                            .build()))
            .filter(response -> response.getFeatureSetsList().size() >= 1)
            .map(response -> response.getFeatureSetsList().get(0))
            .collect(Collectors.toList());

    if (getFeaturesRequest.getFeatureSetsList().size() != featureSetSpecs.size()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Some of the feature sets requested do not exist in Feast. Please check your request payload.")
          .asRuntimeException();
    }

    if (getFeaturesRequest.getEntityDataset().getEntityDatasetRowsCount() < 1) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "entity_dataset_rows is required for batch retrieval in order to filter the retrieved entities.")
          .asRuntimeException();
    }

    for (EntityDatasetRow entityDatasetRow :
        getFeaturesRequest.getEntityDataset().getEntityDatasetRowsList()) {
      if (entityDatasetRow.getEntityTimestamp().getSeconds() == 0) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "entity_timestamp field in entity_dataset_row is required for batch retrieval.")
            .asRuntimeException();
      }
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
              QueryJobConfiguration queryConfig;
              Job queryJob;

              try {
                queryConfig =
                    QueryJobConfiguration.newBuilder(query)
                        .setDefaultDataset(DatasetId.of(projectId, datasetId))
                        .build();
                queryJob = bigquery.create(JobInfo.of(queryConfig));
                jobService.upsert(
                    ServingAPIProto.Job.newBuilder()
                        .setId(feastJobId)
                        .setType(JobType.JOB_TYPE_DOWNLOAD)
                        .setStatus(JobStatus.JOB_STATUS_RUNNING)
                        .build());
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

              try {
                queryConfig = queryJob.getConfiguration();
                String exportTableDestinationUri =
                    String.format("%s/%s/*.avro", jobStagingLocation, feastJobId);

                // Hardcode the format to Avro for now
                ExtractJobConfiguration extractConfig =
                    ExtractJobConfiguration.of(
                        queryConfig.getDestinationTable(), exportTableDestinationUri, "Avro");
                Job extractJob = bigquery.create(JobInfo.of(extractConfig));
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

              String scheme = jobStagingLocation.substring(0, jobStagingLocation.indexOf("://"));
              String stagingLocationNoScheme =
                  jobStagingLocation.substring(jobStagingLocation.indexOf("://") + 3);
              String bucket = stagingLocationNoScheme.split("/")[0];
              List<String> prefixParts = new ArrayList<>();
              prefixParts.add(
                  stagingLocationNoScheme.contains("/") && !stagingLocationNoScheme.endsWith("/")
                      ? stagingLocationNoScheme.substring(stagingLocationNoScheme.indexOf("/") + 1)
                      : "");
              prefixParts.add(feastJobId);
              String prefix = String.join("/", prefixParts) + "/";

              List<String> fileUris = new ArrayList<>();
              for (Blob blob : storage.list(bucket, BlobListOption.prefix(prefix)).iterateAll()) {
                fileUris.add(String.format("%s://%s/%s", scheme, blob.getBucket(), blob.getName()));
              }

              jobService.upsert(
                  ServingAPIProto.Job.newBuilder()
                      .setId(feastJobId)
                      .setType(JobType.JOB_TYPE_DOWNLOAD)
                      .setStatus(JobStatus.JOB_STATUS_DONE)
                      .addAllFileUris(fileUris)
                      .setDataFormat(DataFormat.DATA_FORMAT_AVRO)
                      .build());
            })
        .start();

    return GetBatchFeaturesResponse.newBuilder().setJob(feastJob).build();
  }

  @Override
  public ReloadJobResponse reloadJob(ReloadJobRequest reloadJobRequest) {
    Optional<ServingAPIProto.Job> job = jobService.get(reloadJobRequest.getJob().getId());
    if (!job.isPresent()) {
      throw Status.NOT_FOUND
          .withDescription(String.format("Job not found: %s", reloadJobRequest.getJob().getId()))
          .asRuntimeException();
    }
    return ReloadJobResponse.newBuilder().setJob(job.get()).build();
  }
}
