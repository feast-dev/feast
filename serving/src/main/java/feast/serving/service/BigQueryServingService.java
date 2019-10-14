package feast.serving.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.Lists;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.DataFormat;
import feast.serving.ServingAPIProto.DatasetSource;
import feast.serving.ServingAPIProto.FeastServingType;
import feast.serving.ServingAPIProto.GetBatchFeaturesRequest;
import feast.serving.ServingAPIProto.GetBatchFeaturesResponse;
import feast.serving.ServingAPIProto.GetFeastServingInfoRequest;
import feast.serving.ServingAPIProto.GetFeastServingInfoResponse;
import feast.serving.ServingAPIProto.GetJobRequest;
import feast.serving.ServingAPIProto.GetJobResponse;
import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.serving.ServingAPIProto.JobStatus;
import feast.serving.ServingAPIProto.JobType;
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
  private final CachedSpecService specService;
  private final JobService jobService;
  private final String jobStagingLocation;
  private final Storage storage;

  public BigQueryServingService(
      BigQuery bigquery,
      String projectId,
      String datasetId,
      CachedSpecService specService,
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

  /**
   * {@inheritDoc}
   */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_BATCH)
        .setJobStagingLocation(jobStagingLocation)
        .build();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {

    List<FeatureSetSpec> featureSetSpecs =
        getFeaturesRequest.getFeatureSetsList().stream()
            .map(featureSet ->
                specService.getFeatureSet(featureSet.getName(), featureSet.getVersion())
            )
            .collect(Collectors.toList());

    if (getFeaturesRequest.getFeatureSetsList().size() != featureSetSpecs.size()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Some of the feature sets requested do not exist in Feast. Please check your request payload.")
          .asRuntimeException();
    }

    Table entityTable = loadEntities(getFeaturesRequest.getDatasetSource());
    Schema entityTableSchema = entityTable.getDefinition().getSchema();
    List<String> entityNames = entityTableSchema.getFields().stream()
        .map(Field::getName)
        .filter(name -> !name.equals("event_timestamp"))
        .collect(Collectors.toList());

    final String query =
        BigQueryUtil.createQuery(
            getFeaturesRequest.getFeatureSetsList(),
            featureSetSpecs,
            entityNames,
            datasetId, entityTable.getFriendlyName());
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

  /**
   * {@inheritDoc}
   */
  @Override
  public GetJobResponse getJob(GetJobRequest getJobRequest) {
    Optional<ServingAPIProto.Job> job = jobService.get(getJobRequest.getJob().getId());
    if (!job.isPresent()) {
      throw Status.NOT_FOUND
          .withDescription(String.format("Job not found: %s", getJobRequest.getJob().getId()))
          .asRuntimeException();
    }
    return GetJobResponse.newBuilder().setJob(job.get()).build();
  }

  private Table loadEntities(DatasetSource datasetSource) {
    switch (datasetSource.getDatasetSourceCase()) {
      case FILE_SOURCE:
        String tableName = generateTemporaryTableName();
        TableId tableId = TableId.of(projectId, datasetId, tableName);
        // Currently only avro supported
        if (datasetSource.getFileSource().getDataFormat() != DataFormat.DATA_FORMAT_AVRO) {
          throw Status.INVALID_ARGUMENT
              .withDescription("Invalid file format, only avro supported")
              .asRuntimeException();
        }
        LoadJobConfiguration loadJobConfiguration = LoadJobConfiguration.of(tableId,
            datasetSource.getFileSource().getFileUrisList(),
            FormatOptions.avro());
        Job job = bigquery.create(JobInfo.of(loadJobConfiguration));
        try {
          job.waitFor();
          return bigquery.getTable(tableId);
        } catch (InterruptedException e) {
          throw Status.INTERNAL
              .withDescription("Failed to load entity dataset into store")
              .withCause(e)
              .asRuntimeException();
        }
      case DATASETSOURCE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Data source must be set.")
            .asRuntimeException();
    }
  }

  private String generateTemporaryTableName() {
    String source = String.format("feast_serving_%d", System.currentTimeMillis());
    UUID uuid = UUID.fromString(source);
    return uuid.toString().replaceAll("-", "_");
  }
}
