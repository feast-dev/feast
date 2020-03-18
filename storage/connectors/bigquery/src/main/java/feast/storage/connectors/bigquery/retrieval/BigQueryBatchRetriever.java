/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.storage.connectors.bigquery.retrieval;

import static feast.storage.connectors.bigquery.retrieval.QueryTemplater.createEntityTableUUIDQuery;
import static feast.storage.connectors.bigquery.retrieval.QueryTemplater.createTimestampLimitQuery;

import com.google.auto.value.AutoValue;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import feast.serving.ServingAPIProto;
import feast.storage.api.retrieval.BatchRetriever;
import feast.storage.api.retrieval.FeatureSetRequest;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.threeten.bp.Duration;

@AutoValue
public abstract class BigQueryBatchRetriever implements BatchRetriever {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(BigQueryBatchRetriever.class);

  public static final long TEMP_TABLE_EXPIRY_DURATION_MS = Duration.ofDays(1).toMillis();
  private static final long SUBQUERY_TIMEOUT_SECS = 900; // 15 minutes

  public abstract String projectId();

  public abstract String datasetId();

  public abstract BigQuery bigquery();

  public abstract String jobStagingLocation();

  public abstract int initialRetryDelaySecs();

  public abstract int totalTimeoutSecs();

  public abstract Storage storage();

  public static Builder builder() {
    return new AutoValue_BigQueryBatchRetriever.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract Builder setJobStagingLocation(String jobStagingLocation);

    public abstract Builder setBigquery(BigQuery bigquery);

    public abstract Builder setInitialRetryDelaySecs(int initialRetryDelaySecs);

    public abstract Builder setTotalTimeoutSecs(int totalTimeoutSecs);

    public abstract Builder setStorage(Storage storage);

    public abstract BigQueryBatchRetriever build();
  }

  @Override
  public ServingAPIProto.Job getBatchFeatures(
      ServingAPIProto.GetBatchFeaturesRequest request, List<FeatureSetRequest> featureSetRequests) {
    // 0. Generate job ID
    String feastJobId = UUID.randomUUID().toString();

    List<FeatureSetQueryInfo> featureSetQueryInfos =
        QueryTemplater.getFeatureSetInfos(featureSetRequests);

    // 1. load entity table
    Table entityTable;
    String entityTableName;
    try {
      entityTable = loadEntities(request.getDatasetSource());

      TableId entityTableWithUUIDs = generateUUIDs(entityTable);
      entityTableName = generateFullTableName(entityTableWithUUIDs);
    } catch (Exception e) {
      return ServingAPIProto.Job.newBuilder()
          .setId(feastJobId)
          .setType(ServingAPIProto.JobType.JOB_TYPE_DOWNLOAD)
          .setStatus(ServingAPIProto.JobStatus.JOB_STATUS_DONE)
          .setDataFormat(ServingAPIProto.DataFormat.DATA_FORMAT_AVRO)
          .setError(String.format("Unable to load entity table to BigQuery: %s", e.toString()))
          .build();
    }

    Schema entityTableSchema = entityTable.getDefinition().getSchema();
    List<String> entityTableColumnNames =
        entityTableSchema.getFields().stream()
            .map(Field::getName)
            .filter(name -> !name.equals("event_timestamp"))
            .collect(Collectors.toList());

    // 2. Retrieve the temporal bounds of the entity dataset provided
    FieldValueList timestampLimits = getTimestampLimits(entityTableName);

    // 3. Generate the subqueries
    List<String> featureSetQueries =
        generateQueries(entityTableName, timestampLimits, featureSetQueryInfos);

    QueryJobConfiguration queryConfig;

    try {
      // 4. Run the subqueries in parallel then collect the outputs
      Job queryJob =
          runBatchQuery(
              entityTableName, entityTableColumnNames, featureSetQueryInfos, featureSetQueries);
      queryConfig = queryJob.getConfiguration();
      String exportTableDestinationUri =
          String.format("%s/%s/*.avro", jobStagingLocation(), feastJobId);

      // 5. Export the table
      // Hardcode the format to Avro for now
      ExtractJobConfiguration extractConfig =
          ExtractJobConfiguration.of(
              queryConfig.getDestinationTable(), exportTableDestinationUri, "Avro");
      Job extractJob = bigquery().create(JobInfo.of(extractConfig));
      waitForJob(extractJob);

    } catch (BigQueryException | InterruptedException | IOException e) {
      return ServingAPIProto.Job.newBuilder()
          .setId(feastJobId)
          .setType(ServingAPIProto.JobType.JOB_TYPE_DOWNLOAD)
          .setStatus(ServingAPIProto.JobStatus.JOB_STATUS_DONE)
          .setError(e.getMessage())
          .build();
    }

    List<String> fileUris = parseOutputFileURIs(feastJobId);

    return ServingAPIProto.Job.newBuilder()
        .setId(feastJobId)
        .setType(ServingAPIProto.JobType.JOB_TYPE_DOWNLOAD)
        .setStatus(ServingAPIProto.JobStatus.JOB_STATUS_DONE)
        .addAllFileUris(fileUris)
        .setDataFormat(ServingAPIProto.DataFormat.DATA_FORMAT_AVRO)
        .build();
  }

  private TableId generateUUIDs(Table loadedEntityTable) {
    try {
      String uuidQuery =
          createEntityTableUUIDQuery(generateFullTableName(loadedEntityTable.getTableId()));
      QueryJobConfiguration queryJobConfig =
          QueryJobConfiguration.newBuilder(uuidQuery)
              .setDestinationTable(TableId.of(projectId(), datasetId(), createTempTableName()))
              .build();
      Job queryJob = bigquery().create(JobInfo.of(queryJobConfig));
      Job completedJob = waitForJob(queryJob);
      TableInfo expiry =
          bigquery()
              .getTable(queryJobConfig.getDestinationTable())
              .toBuilder()
              .setExpirationTime(System.currentTimeMillis() + TEMP_TABLE_EXPIRY_DURATION_MS)
              .build();
      bigquery().update(expiry);
      queryJobConfig = completedJob.getConfiguration();
      return queryJobConfig.getDestinationTable();
    } catch (InterruptedException | BigQueryException e) {
      throw Status.INTERNAL
          .withDescription("Failed to load entity dataset into store")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private FieldValueList getTimestampLimits(String entityTableName) {
    QueryJobConfiguration getTimestampLimitsQuery =
        QueryJobConfiguration.newBuilder(createTimestampLimitQuery(entityTableName))
            .setDefaultDataset(DatasetId.of(projectId(), datasetId()))
            .setDestinationTable(TableId.of(projectId(), datasetId(), createTempTableName()))
            .build();
    try {
      Job job = bigquery().create(JobInfo.of(getTimestampLimitsQuery));
      TableResult getTimestampLimitsQueryResult = waitForJob(job).getQueryResults();
      TableInfo expiry =
          bigquery()
              .getTable(getTimestampLimitsQuery.getDestinationTable())
              .toBuilder()
              .setExpirationTime(System.currentTimeMillis() + TEMP_TABLE_EXPIRY_DURATION_MS)
              .build();
      bigquery().update(expiry);
      FieldValueList result = null;
      for (FieldValueList fields : getTimestampLimitsQueryResult.getValues()) {
        result = fields;
      }
      if (result == null || result.get("min").isNull() || result.get("max").isNull()) {
        throw new RuntimeException("query returned insufficient values");
      }
      return result;
    } catch (InterruptedException e) {
      throw Status.INTERNAL
          .withDescription("Unable to extract min and max timestamps from query")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private Table loadEntities(ServingAPIProto.DatasetSource datasetSource) {
    Table loadedEntityTable;
    switch (datasetSource.getDatasetSourceCase()) {
      case FILE_SOURCE:
        try {
          // Currently only AVRO format is supported
          if (datasetSource.getFileSource().getDataFormat()
              != ServingAPIProto.DataFormat.DATA_FORMAT_AVRO) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Invalid file format, only AVRO is supported.")
                .asRuntimeException();
          }

          TableId tableId = TableId.of(projectId(), datasetId(), createTempTableName());
          log.info(
              "Loading entity rows to: {}.{}.{}", projectId(), datasetId(), tableId.getTable());

          LoadJobConfiguration loadJobConfiguration =
              LoadJobConfiguration.of(
                  tableId, datasetSource.getFileSource().getFileUrisList(), FormatOptions.avro());
          loadJobConfiguration =
              loadJobConfiguration.toBuilder().setUseAvroLogicalTypes(true).build();
          Job job = bigquery().create(JobInfo.of(loadJobConfiguration));
          waitForJob(job);

          TableInfo expiry =
              bigquery()
                  .getTable(tableId)
                  .toBuilder()
                  .setExpirationTime(System.currentTimeMillis() + TEMP_TABLE_EXPIRY_DURATION_MS)
                  .build();
          bigquery().update(expiry);

          loadedEntityTable = bigquery().getTable(tableId);
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

  private List<String> generateQueries(
      String entityTableName,
      FieldValueList timestampLimits,
      List<FeatureSetQueryInfo> featureSetQueryInfos) {
    List<String> featureSetQueries = new ArrayList<>();
    try {
      for (FeatureSetQueryInfo featureSetInfo : featureSetQueryInfos) {
        String query =
            QueryTemplater.createFeatureSetPointInTimeQuery(
                featureSetInfo,
                projectId(),
                datasetId(),
                entityTableName,
                timestampLimits.get("min").getStringValue(),
                timestampLimits.get("max").getStringValue());
        featureSetQueries.add(query);
      }
    } catch (IOException e) {
      throw Status.INTERNAL
          .withDescription("Unable to generate query for batch retrieval")
          .withCause(e)
          .asRuntimeException();
    }
    return featureSetQueries;
  }

  Job runBatchQuery(
      String entityTableName,
      List<String> entityTableColumnNames,
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> featureSetQueries)
      throws BigQueryException, InterruptedException, IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(featureSetQueries.size());
    ExecutorCompletionService<FeatureSetQueryInfo> executorCompletionService =
        new ExecutorCompletionService<>(executorService);

    // For each of the feature sets requested, start an async job joining the features in that
    // feature set to the provided entity table
    for (int i = 0; i < featureSetQueries.size(); i++) {
      QueryJobConfiguration queryJobConfig =
          QueryJobConfiguration.newBuilder(featureSetQueries.get(i))
              .setDestinationTable(TableId.of(projectId(), datasetId(), createTempTableName()))
              .build();
      Job subqueryJob = bigquery().create(JobInfo.of(queryJobConfig));
      executorCompletionService.submit(
          SubqueryCallable.builder()
              .setBigquery(bigquery())
              .setFeatureSetInfo(featureSetQueryInfos.get(i))
              .setSubqueryJob(subqueryJob)
              .build());
    }

    List<FeatureSetQueryInfo> completedFeatureSetQueryInfos = new ArrayList<>();

    for (int i = 0; i < featureSetQueries.size(); i++) {
      try {
        // Try to retrieve the outputs of all the jobs. The timeout here is a formality;
        // a stricter timeout is implemented in the actual SubqueryCallable.
        FeatureSetQueryInfo featureSetInfo =
            executorCompletionService.take().get(SUBQUERY_TIMEOUT_SECS, TimeUnit.SECONDS);
        completedFeatureSetQueryInfos.add(featureSetInfo);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        executorService.shutdownNow();
        throw Status.INTERNAL
            .withDescription("Error running batch query")
            .withCause(e)
            .asRuntimeException();
      }
    }

    // Generate and run a join query to collect the outputs of all the
    // subqueries into a single table.
    String joinQuery =
        QueryTemplater.createJoinQuery(
            completedFeatureSetQueryInfos, entityTableColumnNames, entityTableName);
    QueryJobConfiguration queryJobConfig =
        QueryJobConfiguration.newBuilder(joinQuery)
            .setDestinationTable(TableId.of(projectId(), datasetId(), createTempTableName()))
            .build();
    Job queryJob = bigquery().create(JobInfo.of(queryJobConfig));
    Job completedQueryJob = waitForJob(queryJob);

    TableInfo expiry =
        bigquery()
            .getTable(queryJobConfig.getDestinationTable())
            .toBuilder()
            .setExpirationTime(System.currentTimeMillis() + TEMP_TABLE_EXPIRY_DURATION_MS)
            .build();
    bigquery().update(expiry);

    return completedQueryJob;
  }

  private List<String> parseOutputFileURIs(String feastJobId) {
    String scheme = jobStagingLocation().substring(0, jobStagingLocation().indexOf("://"));
    String stagingLocationNoScheme =
        jobStagingLocation().substring(jobStagingLocation().indexOf("://") + 3);
    String bucket = stagingLocationNoScheme.split("/")[0];
    List<String> prefixParts = new ArrayList<>();
    prefixParts.add(
        stagingLocationNoScheme.contains("/") && !stagingLocationNoScheme.endsWith("/")
            ? stagingLocationNoScheme.substring(stagingLocationNoScheme.indexOf("/") + 1)
            : "");
    prefixParts.add(feastJobId);
    String prefix = String.join("/", prefixParts) + "/";

    List<String> fileUris = new ArrayList<>();
    for (Blob blob : storage().list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
      fileUris.add(String.format("%s://%s/%s", scheme, blob.getBucket(), blob.getName()));
    }
    return fileUris;
  }

  private Job waitForJob(Job queryJob) throws InterruptedException {
    Job completedJob =
        queryJob.waitFor(
            RetryOption.initialRetryDelay(Duration.ofSeconds(initialRetryDelaySecs())),
            RetryOption.totalTimeout(Duration.ofSeconds(totalTimeoutSecs())));
    if (completedJob == null) {
      throw Status.INTERNAL.withDescription("Job no longer exists").asRuntimeException();
    } else if (completedJob.getStatus().getError() != null) {
      throw Status.INTERNAL
          .withDescription("Job failed: " + completedJob.getStatus().getError())
          .asRuntimeException();
    }
    return completedJob;
  }

  public String generateFullTableName(TableId tableId) {
    return String.format(
        "%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  public String createTempTableName() {
    return "_" + UUID.randomUUID().toString().replace("-", "");
  }
}
