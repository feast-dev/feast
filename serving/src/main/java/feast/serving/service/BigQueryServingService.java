/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.service;

import static feast.serving.store.bigquery.QueryTemplater.createEntityTableUUIDQuery;
import static feast.serving.store.bigquery.QueryTemplater.generateFullTableName;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.storage.Storage;
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
import feast.serving.specs.CachedSpecService;
import feast.serving.specs.FeatureSetRequest;
import feast.serving.store.bigquery.BatchRetrievalQueryRunnable;
import feast.serving.store.bigquery.QueryTemplater;
import feast.serving.store.bigquery.model.FeatureSetInfo;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.threeten.bp.Duration;

public class BigQueryServingService implements ServingService {

  public static final long TEMP_TABLE_EXPIRY_DURATION_MS = Duration.ofDays(1).toMillis();
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(BigQueryServingService.class);

  private final BigQuery bigquery;
  private final String projectId;
  private final String datasetId;
  private final CachedSpecService specService;
  private final JobService jobService;
  private final String jobStagingLocation;
  private final int initialRetryDelaySecs;
  private final int totalTimeoutSecs;
  private final Storage storage;

  public BigQueryServingService(
      BigQuery bigquery,
      String projectId,
      String datasetId,
      CachedSpecService specService,
      JobService jobService,
      String jobStagingLocation,
      int initialRetryDelaySecs,
      int totalTimeoutSecs,
      Storage storage) {
    this.bigquery = bigquery;
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.specService = specService;
    this.jobService = jobService;
    this.jobStagingLocation = jobStagingLocation;
    this.initialRetryDelaySecs = initialRetryDelaySecs;
    this.totalTimeoutSecs = totalTimeoutSecs;
    this.storage = storage;
  }

  /** {@inheritDoc} */
  @Override
  public GetFeastServingInfoResponse getFeastServingInfo(
      GetFeastServingInfoRequest getFeastServingInfoRequest) {
    return GetFeastServingInfoResponse.newBuilder()
        .setType(FeastServingType.FEAST_SERVING_TYPE_BATCH)
        .setJobStagingLocation(jobStagingLocation)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest getFeaturesRequest) {
    throw Status.UNIMPLEMENTED.withDescription("Method not implemented").asRuntimeException();
  }

  /** {@inheritDoc} */
  @Override
  public GetBatchFeaturesResponse getBatchFeatures(GetBatchFeaturesRequest getFeaturesRequest) {
    List<FeatureSetRequest> featureSetRequests =
        specService.getFeatureSets(getFeaturesRequest.getFeaturesList());

    Table entityTable;
    String entityTableName;
    try {
      entityTable = loadEntities(getFeaturesRequest.getDatasetSource());

      TableId entityTableWithUUIDs = generateUUIDs(entityTable);
      entityTableName = generateFullTableName(entityTableWithUUIDs);
    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("Unable to load entity dataset to Bigquery")
          .asRuntimeException();
    }

    Schema entityTableSchema = entityTable.getDefinition().getSchema();
    List<String> entityNames =
        entityTableSchema.getFields().stream()
            .map(Field::getName)
            .filter(name -> !name.equals("event_timestamp"))
            .collect(Collectors.toList());

    List<FeatureSetInfo> featureSetInfos = QueryTemplater.getFeatureSetInfos(featureSetRequests);

    String feastJobId = UUID.randomUUID().toString();
    ServingAPIProto.Job feastJob =
        ServingAPIProto.Job.newBuilder()
            .setId(feastJobId)
            .setType(JobType.JOB_TYPE_DOWNLOAD)
            .setStatus(JobStatus.JOB_STATUS_PENDING)
            .build();
    jobService.upsert(feastJob);

    new Thread(
            BatchRetrievalQueryRunnable.builder()
                .setEntityTableName(entityTableName)
                .setBigquery(bigquery)
                .setStorage(storage)
                .setJobService(jobService)
                .setProjectId(projectId)
                .setDatasetId(datasetId)
                .setFeastJobId(feastJobId)
                .setEntityTableColumnNames(entityNames)
                .setFeatureSetInfos(featureSetInfos)
                .setJobStagingLocation(jobStagingLocation)
                .setInitialRetryDelaySecs(initialRetryDelaySecs)
                .setTotalTimeoutSecs(totalTimeoutSecs)
                .build())
        .start();

    return GetBatchFeaturesResponse.newBuilder().setJob(feastJob).build();
  }

  /** {@inheritDoc} */
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
    Table loadedEntityTable;
    switch (datasetSource.getDatasetSourceCase()) {
      case FILE_SOURCE:
        try {
          // Currently only AVRO format is supported

          if (datasetSource.getFileSource().getDataFormat() != DataFormat.DATA_FORMAT_AVRO) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Invalid file format, only AVRO is supported.")
                .asRuntimeException();
          }

          TableId tableId = TableId.of(projectId, datasetId, createTempTableName());
          log.info("Loading entity rows to: {}.{}.{}", projectId, datasetId, tableId.getTable());

          LoadJobConfiguration loadJobConfiguration =
              LoadJobConfiguration.of(
                  tableId, datasetSource.getFileSource().getFileUrisList(), FormatOptions.avro());
          loadJobConfiguration =
              loadJobConfiguration.toBuilder().setUseAvroLogicalTypes(true).build();
          Job job = bigquery.create(JobInfo.of(loadJobConfiguration));
          waitForJob(job);

          TableInfo expiry =
              bigquery
                  .getTable(tableId)
                  .toBuilder()
                  .setExpirationTime(System.currentTimeMillis() + TEMP_TABLE_EXPIRY_DURATION_MS)
                  .build();
          bigquery.update(expiry);

          loadedEntityTable = bigquery.getTable(tableId);
          if (!loadedEntityTable.exists()) {
            throw new RuntimeException(
                "Unable to create entity dataset table, table already exists");
          }
          return loadedEntityTable;
        } catch (Exception e) {
          log.error("Exception has occurred in loadEntities method: ", e);
          throw Status.INTERNAL
              .withDescription("Failed to load entity dataset into store: " + e.toString())
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

  private TableId generateUUIDs(Table loadedEntityTable) {
    try {
      String uuidQuery =
          createEntityTableUUIDQuery(generateFullTableName(loadedEntityTable.getTableId()));
      QueryJobConfiguration queryJobConfig =
          QueryJobConfiguration.newBuilder(uuidQuery)
              .setDestinationTable(TableId.of(projectId, datasetId, createTempTableName()))
              .build();
      Job queryJob = bigquery.create(JobInfo.of(queryJobConfig));
      Job completedJob = waitForJob(queryJob);
      TableInfo expiry =
          bigquery
              .getTable(queryJobConfig.getDestinationTable())
              .toBuilder()
              .setExpirationTime(System.currentTimeMillis() + TEMP_TABLE_EXPIRY_DURATION_MS)
              .build();
      bigquery.update(expiry);
      queryJobConfig = completedJob.getConfiguration();
      return queryJobConfig.getDestinationTable();
    } catch (InterruptedException | BigQueryException e) {
      throw Status.INTERNAL
          .withDescription("Failed to load entity dataset into store")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private Job waitForJob(Job queryJob) throws InterruptedException {
    Job completedJob =
        queryJob.waitFor(
            RetryOption.initialRetryDelay(Duration.ofSeconds(initialRetryDelaySecs)),
            RetryOption.totalTimeout(Duration.ofSeconds(totalTimeoutSecs)));
    if (completedJob == null) {
      throw Status.INTERNAL.withDescription("Job no longer exists").asRuntimeException();
    } else if (completedJob.getStatus().getError() != null) {
      throw Status.INTERNAL
          .withDescription("Job failed: " + completedJob.getStatus().getError())
          .asRuntimeException();
    }
    return completedJob;
  }

  public static String createTempTableName() {
    return "_" + UUID.randomUUID().toString().replace("-", "");
  }
}
