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
package feast.core.job;

import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.SourceProto;
import feast.core.StoreProto;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.FeatureSet;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * JobUpdateTask is a callable that starts or updates a job given a set of featureSetSpecs, as well
 * as their source and sink.
 *
 * <p>When complete, the JobUpdateTask returns the updated JobInfo object to be pushed to the db.
 */
@Slf4j
@Getter
public class JobUpdateTask implements Callable<JobInfo> {

  private final long JOB_UPDATE_TIMEOUT_SECONDS = 240; // 4 minutes

  private final List<FeatureSetSpec> featureSetSpecs;
  private final SourceProto.Source sourceSpec;
  private final StoreProto.Store store;
  private final Optional<JobInfo> originalJob;
  private JobManager jobManager;

  public JobUpdateTask(
      List<FeatureSetSpec> featureSetSpecs,
      SourceProto.Source sourceSpec,
      StoreProto.Store store,
      Optional<JobInfo> originalJob,
      JobManager jobManager) {

    this.featureSetSpecs = featureSetSpecs;
    this.sourceSpec = sourceSpec;
    this.store = store;
    this.originalJob = originalJob;
    this.jobManager = jobManager;
  }

  @Override
  public JobInfo call() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Source source = Source.fromProto(sourceSpec);
    Future<JobInfo> submittedJob;
    if (originalJob.isPresent()) {
      Set<String> existingFeatureSetsPopulatedByJob =
          originalJob.get().getFeatureSets().stream()
              .map(FeatureSet::getId)
              .collect(Collectors.toSet());
      Set<String> newFeatureSetsPopulatedByJob =
          featureSetSpecs.stream()
              .map(fs -> fs.getName() + ":" + fs.getVersion())
              .collect(Collectors.toSet());
      if (existingFeatureSetsPopulatedByJob.size() == newFeatureSetsPopulatedByJob.size()
          && existingFeatureSetsPopulatedByJob.containsAll(newFeatureSetsPopulatedByJob)) {
        JobInfo job = originalJob.get();
        JobStatus newJobStatus = jobManager.getJobStatus(job);
        if (newJobStatus != job.getStatus()) {
          AuditLogger.log(
              Resource.JOB,
              job.getId(),
              Action.STATUS_CHANGE,
              "Job status updated: changed from %s to %s",
              job.getStatus(),
              newJobStatus);
        }
        job.setStatus(newJobStatus);
        return job;
      } else {
        submittedJob =
            executorService.submit(() -> updateJob(originalJob.get(), featureSetSpecs, store));
      }
    } else {
      String jobId = createJobId(source.getId(), store.getName());
      submittedJob =
          executorService.submit(() -> startJob(jobId, featureSetSpecs, sourceSpec, store));
    }

    JobInfo job = null;
    try {
      job = submittedJob.get(getJobUpdateTimeoutSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Unable to start job for source {} and sink {}: {}", source, store, e.getMessage());
      executorService.shutdownNow();
    }
    return job;
  }

  /** Start or update the job to ingest data to the sink. */
  private JobInfo startJob(
      String jobId,
      List<FeatureSetSpec> featureSetSpecs,
      SourceProto.Source source,
      StoreProto.Store sinkSpec) {

    List<FeatureSet> featureSets =
        featureSetSpecs.stream()
            .map(
                spec -> {
                  FeatureSet featureSet = new FeatureSet();
                  featureSet.setId(spec.getName() + ":" + spec.getVersion());
                  return featureSet;
                })
            .collect(Collectors.toList());
    String extId = "";
    try {
      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.SUBMIT,
          "Building graph and submitting to %s",
          jobManager.getRunnerType().getName());

      extId = jobManager.startJob(jobId, featureSetSpecs, sinkSpec);
      if (extId.isEmpty()) {
        throw new RuntimeException(
            String.format("Could not submit job: \n%s", "unable to retrieve job external id"));
      }

      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.STATUS_CHANGE,
          "Job submitted to runner %s with ext id %s.",
          jobManager.getRunnerType().getName(),
          extId);

      return new JobInfo(
          jobId,
          extId,
          jobManager.getRunnerType().getName(),
          feast.core.model.Source.fromProto(source),
          feast.core.model.Store.fromProto(sinkSpec),
          featureSets,
          JobStatus.RUNNING);
    } catch (Exception e) {
      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.STATUS_CHANGE,
          "Job failed to be submitted to runner %s. Job status changed to ERROR.",
          jobManager.getRunnerType().getName());

      return new JobInfo(
          jobId,
          extId,
          jobManager.getRunnerType().getName(),
          feast.core.model.Source.fromProto(source),
          feast.core.model.Store.fromProto(sinkSpec),
          featureSets,
          JobStatus.ERROR);
    }
  }

  /** Update the given job */
  private JobInfo updateJob(
      JobInfo jobInfo, List<FeatureSetSpec> featureSetSpecs, StoreProto.Store store) {
    jobInfo.setFeatureSets(
        featureSetSpecs.stream()
            .map(spec -> FeatureSet.fromSpec(spec))
            .collect(Collectors.toList()));
    jobInfo.setStore(feast.core.model.Store.fromProto(store));
    AuditLogger.log(
        Resource.JOB,
        jobInfo.getId(),
        Action.UPDATE,
        "Updating job %s for runner %s",
        jobInfo.getId(),
        jobManager.getRunnerType().getName());
    String extId = jobManager.updateJob(jobInfo);
    jobInfo.setExtId(extId);
    return jobInfo;
  }

  String createJobId(String sourceId, String storeName) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String sourceIdTrunc = sourceId.split("/")[0].toLowerCase();
    String jobId = String.format("%s-to-%s", sourceIdTrunc, storeName) + dateSuffix;
    return jobId.replaceAll("_", "-");
  }

  long getJobUpdateTimeoutSeconds() {
    return JOB_UPDATE_TIMEOUT_SECONDS;
  }
}
