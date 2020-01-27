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
package feast.serving.store.bigquery;

import static feast.serving.service.BigQueryServingService.TEMP_TABLE_EXPIRY_DURATION_MS;
import static feast.serving.service.BigQueryServingService.createTempTableName;
import static feast.serving.store.bigquery.QueryTemplater.createTimestampLimitQuery;

import com.google.auto.value.AutoValue;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.DataFormat;
import feast.serving.ServingAPIProto.JobStatus;
import feast.serving.ServingAPIProto.JobType;
import feast.serving.service.JobService;
import feast.serving.store.bigquery.model.FeatureSetInfo;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.threeten.bp.Duration;

/**
 * BatchRetrievalQueryRunnable is a Runnable for running a BigQuery Feast batch retrieval job async.
 *
 * <p>It does the following, in sequence:
 *
 * <p>1. Retrieve the temporal bounds of the entity dataset provided. This will be used to filter
 * the feature set tables when performing the feature retrieval.
 *
 * <p>2. For each of the feature sets requested, generate the subquery for doing a point-in-time
 * correctness join of the features in the feature set to the entity table.
 *
 * <p>3. Run each of the subqueries in parallel and wait for them to complete. If any of the jobs
 * are unsuccessful, the thread running the BatchRetrievalQueryRunnable catches the error and
 * updates the job database.
 *
 * <p>4. When all the subquery jobs are complete, join the outputs of all the subqueries into a
 * single table.
 *
 * <p>5. Extract the output of the join to a remote file, and write the location of the remote file
 * to the job database, and mark the retrieval job as successful.
 */
@AutoValue
public abstract class BatchRetrievalQueryRunnable implements Runnable {

  private static final long SUBQUERY_TIMEOUT_SECS = 900; // 15 minutes

  public abstract JobService jobService();

  public abstract String projectId();

  public abstract String datasetId();

  public abstract String feastJobId();

  public abstract BigQuery bigquery();

  public abstract List<String> entityTableColumnNames();

  public abstract List<FeatureSetInfo> featureSetInfos();

  public abstract String entityTableName();

  public abstract String jobStagingLocation();

  public abstract int initialRetryDelaySecs();

  public abstract int totalTimeoutSecs();

  public abstract Storage storage();

  public static Builder builder() {
    return new AutoValue_BatchRetrievalQueryRunnable.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setJobService(JobService jobService);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract Builder setFeastJobId(String feastJobId);

    public abstract Builder setBigquery(BigQuery bigquery);

    public abstract Builder setEntityTableColumnNames(List<String> entityTableColumnNames);

    public abstract Builder setFeatureSetInfos(List<FeatureSetInfo> featureSetInfos);

    public abstract Builder setEntityTableName(String entityTableName);

    public abstract Builder setJobStagingLocation(String jobStagingLocation);

    public abstract Builder setInitialRetryDelaySecs(int initialRetryDelaySecs);

    public abstract Builder setTotalTimeoutSecs(int totalTimeoutSecs);

    public abstract Builder setStorage(Storage storage);

    public abstract BatchRetrievalQueryRunnable build();
  }

  @Override
  public void run() {

    // 1. Retrieve the temporal bounds of the entity dataset provided
    FieldValueList timestampLimits = getTimestampLimits(entityTableName());

    // 2. Generate the subqueries
    List<String> featureSetQueries = generateQueries(timestampLimits);

    QueryJobConfiguration queryConfig;

    try {
      // 3 & 4. Run the subqueries in parallel then collect the outputs
      Job queryJob = runBatchQuery(featureSetQueries);
      queryConfig = queryJob.getConfiguration();
      String exportTableDestinationUri =
          String.format("%s/%s/*.avro", jobStagingLocation(), feastJobId());

      // 5. Export the table
      // Hardcode the format to Avro for now
      ExtractJobConfiguration extractConfig =
          ExtractJobConfiguration.of(
              queryConfig.getDestinationTable(), exportTableDestinationUri, "Avro");
      Job extractJob = bigquery().create(JobInfo.of(extractConfig));
      waitForJob(extractJob);
    } catch (BigQueryException | InterruptedException | IOException e) {
      jobService()
          .upsert(
              ServingAPIProto.Job.newBuilder()
                  .setId(feastJobId())
                  .setType(JobType.JOB_TYPE_DOWNLOAD)
                  .setStatus(JobStatus.JOB_STATUS_DONE)
                  .setError(e.getMessage())
                  .build());
      return;
    }

    List<String> fileUris = parseOutputFileURIs();

    // 5. Update the job database
    jobService()
        .upsert(
            ServingAPIProto.Job.newBuilder()
                .setId(feastJobId())
                .setType(JobType.JOB_TYPE_DOWNLOAD)
                .setStatus(JobStatus.JOB_STATUS_DONE)
                .addAllFileUris(fileUris)
                .setDataFormat(DataFormat.DATA_FORMAT_AVRO)
                .build());
  }

  private List<String> parseOutputFileURIs() {
    String scheme = jobStagingLocation().substring(0, jobStagingLocation().indexOf("://"));
    String stagingLocationNoScheme =
        jobStagingLocation().substring(jobStagingLocation().indexOf("://") + 3);
    String bucket = stagingLocationNoScheme.split("/")[0];
    List<String> prefixParts = new ArrayList<>();
    prefixParts.add(
        stagingLocationNoScheme.contains("/") && !stagingLocationNoScheme.endsWith("/")
            ? stagingLocationNoScheme.substring(stagingLocationNoScheme.indexOf("/") + 1)
            : "");
    prefixParts.add(feastJobId());
    String prefix = String.join("/", prefixParts) + "/";

    List<String> fileUris = new ArrayList<>();
    for (Blob blob : storage().list(bucket, BlobListOption.prefix(prefix)).iterateAll()) {
      fileUris.add(String.format("%s://%s/%s", scheme, blob.getBucket(), blob.getName()));
    }
    return fileUris;
  }

  Job runBatchQuery(List<String> featureSetQueries)
      throws BigQueryException, InterruptedException, IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(featureSetQueries.size());
    ExecutorCompletionService<FeatureSetInfo> executorCompletionService =
        new ExecutorCompletionService<>(executorService);

    List<FeatureSetInfo> featureSetInfos = new ArrayList<>();

    // For each of the feature sets requested, start an async job joining the features in that
    // feature set to the provided entity table
    for (int i = 0; i < featureSetQueries.size(); i++) {
      System.out.println(featureSetQueries.get(i));
      QueryJobConfiguration queryJobConfig =
          QueryJobConfiguration.newBuilder(featureSetQueries.get(i))
              .setDestinationTable(TableId.of(projectId(), datasetId(), createTempTableName()))
              .build();
      Job subqueryJob = bigquery().create(JobInfo.of(queryJobConfig));
      executorCompletionService.submit(
          SubqueryCallable.builder()
              .setBigquery(bigquery())
              .setFeatureSetInfo(featureSetInfos().get(i))
              .setSubqueryJob(subqueryJob)
              .build());
    }

    for (int i = 0; i < featureSetQueries.size(); i++) {
      System.out.println(i);
      try {
        // Try to retrieve the outputs of all the jobs. The timeout here is a formality;
        // a stricter timeout is implemented in the actual SubqueryCallable.
        FeatureSetInfo featureSetInfo =
            executorCompletionService.take().get(SUBQUERY_TIMEOUT_SECS, TimeUnit.SECONDS);
        featureSetInfos.add(featureSetInfo);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        jobService()
            .upsert(
                ServingAPIProto.Job.newBuilder()
                    .setId(feastJobId())
                    .setType(JobType.JOB_TYPE_DOWNLOAD)
                    .setStatus(JobStatus.JOB_STATUS_DONE)
                    .setError(e.getMessage())
                    .build());

        executorService.shutdownNow();
        e.printStackTrace();
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
            featureSetInfos, entityTableColumnNames(), entityTableName());
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

  private List<String> generateQueries(FieldValueList timestampLimits) {
    List<String> featureSetQueries = new ArrayList<>();
    try {
      for (FeatureSetInfo featureSetInfo : featureSetInfos()) {
        String query =
            QueryTemplater.createFeatureSetPointInTimeQuery(
                featureSetInfo,
                projectId(),
                datasetId(),
                entityTableName(),
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
}
