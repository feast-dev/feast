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

import feast.core.FeatureSetProto;
import feast.core.SourceProto;
import feast.core.StoreProto;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.model.Source;
import feast.core.model.Store;
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
 * <p>When complete, the JobUpdateTask returns the updated Job object to be pushed to the db.
 */
@Slf4j
@Getter
public class JobUpdateTask implements Callable<Job> {

  private final List<FeatureSetProto.FeatureSet> featureSets;
  private final SourceProto.Source sourceSpec;
  private final StoreProto.Store store;
  private final Optional<Job> currentJob;
  private JobManager jobManager;
  private long jobUpdateTimeoutSeconds;

  public JobUpdateTask(
      List<FeatureSetProto.FeatureSet> featureSets,
      SourceProto.Source sourceSpec,
      StoreProto.Store store,
      Optional<Job> currentJob,
      JobManager jobManager,
      long jobUpdateTimeoutSeconds) {

    this.featureSets = featureSets;
    this.sourceSpec = sourceSpec;
    this.store = store;
    this.currentJob = currentJob;
    this.jobManager = jobManager;
    this.jobUpdateTimeoutSeconds = jobUpdateTimeoutSeconds;
  }

  @Override
  public Job call() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Source source = Source.fromProto(sourceSpec);
    Future<Job> submittedJob;
    if (currentJob.isPresent()) {
      Set<String> existingFeatureSetsPopulatedByJob =
          currentJob.get().getFeatureSets().stream()
              .map(FeatureSet::getId)
              .collect(Collectors.toSet());
      Set<String> newFeatureSetsPopulatedByJob =
          featureSets.stream()
              .map(fs -> FeatureSet.fromProto(fs).getId())
              .collect(Collectors.toSet());
      if (existingFeatureSetsPopulatedByJob.size() == newFeatureSetsPopulatedByJob.size()
          && existingFeatureSetsPopulatedByJob.containsAll(newFeatureSetsPopulatedByJob)) {
        Job job = currentJob.get();
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
            executorService.submit(() -> updateJob(currentJob.get(), featureSets, store));
      }
    } else {
      String jobId = createJobId(source.getId(), store.getName());
      submittedJob = executorService.submit(() -> startJob(jobId, featureSets, sourceSpec, store));
    }

    Job job = null;
    try {
      job = submittedJob.get(getJobUpdateTimeoutSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Unable to start job for source {} and sink {}: {}", source, store, e.getMessage());
      executorService.shutdownNow();
    }
    return job;
  }

  /** Start or update the job to ingest data to the sink. */
  private Job startJob(
      String jobId,
      List<FeatureSetProto.FeatureSet> featureSetProtos,
      SourceProto.Source source,
      StoreProto.Store sinkSpec) {

    List<FeatureSet> featureSets =
        featureSetProtos.stream()
            .map(
                fsp ->
                    FeatureSet.fromProto(
                        FeatureSetProto.FeatureSet.newBuilder()
                            .setSpec(fsp.getSpec())
                            .setMeta(fsp.getMeta())
                            .build()))
            .collect(Collectors.toList());
    Job job =
        new Job(
            jobId,
            "",
            jobManager.getRunnerType().name(),
            Source.fromProto(source),
            Store.fromProto(sinkSpec),
            featureSets,
            JobStatus.PENDING);
    try {
      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.SUBMIT,
          "Building graph and submitting to %s",
          jobManager.getRunnerType().toString());

      job = jobManager.startJob(job);
      if (job.getExtId().isEmpty()) {
        throw new RuntimeException(
            String.format("Could not submit job: \n%s", "unable to retrieve job external id"));
      }

      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.STATUS_CHANGE,
          "Job submitted to runner %s with ext id %s.",
          jobManager.getRunnerType().toString(),
          job.getExtId());

      return job;
    } catch (Exception e) {
      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.STATUS_CHANGE,
          "Job failed to be submitted to runner %s. Job status changed to ERROR.",
          jobManager.getRunnerType().toString());

      job.setStatus(JobStatus.ERROR);
      return job;
    }
  }

  /** Update the given job */
  private Job updateJob(
      Job job, List<FeatureSetProto.FeatureSet> featureSets, StoreProto.Store store) {
    job.setFeatureSets(
        featureSets.stream()
            .map(
                fs ->
                    FeatureSet.fromProto(
                        FeatureSetProto.FeatureSet.newBuilder()
                            .setSpec(fs.getSpec())
                            .setMeta(fs.getMeta())
                            .build()))
            .collect(Collectors.toList()));
    job.setStore(feast.core.model.Store.fromProto(store));
    AuditLogger.log(
        Resource.JOB,
        job.getId(),
        Action.UPDATE,
        "Updating job %s for runner %s",
        job.getId(),
        jobManager.getRunnerType().toString());
    return jobManager.updateJob(job);
  }

  String createJobId(String sourceId, String storeName) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String sourceIdTrunc = sourceId.split("/")[0].toLowerCase();
    String jobId = String.format("%s-to-%s", sourceIdTrunc, storeName) + dateSuffix;
    return jobId.replaceAll("_", "-");
  }
}
