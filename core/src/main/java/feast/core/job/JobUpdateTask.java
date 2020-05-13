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

import com.google.common.collect.Sets;
import feast.core.FeatureSetProto.FeatureSetStatus;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  private final List<FeatureSet> featureSets;
  private final Source source;
  private final Store store;
  private final Optional<Job> currentJob;
  private final JobManager jobManager;
  private final long jobUpdateTimeoutSeconds;
  private final String runnerName;

  public JobUpdateTask(
      List<FeatureSet> featureSets,
      Source source,
      Store store,
      Optional<Job> currentJob,
      JobManager jobManager,
      long jobUpdateTimeoutSeconds) {

    this.featureSets = featureSets;
    this.source = source;
    this.store = store;
    this.currentJob = currentJob;
    this.jobManager = jobManager;
    this.jobUpdateTimeoutSeconds = jobUpdateTimeoutSeconds;
    this.runnerName = jobManager.getRunnerType().toString();
  }

  @Override
  public Job call() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Job> submittedJob;

    if (currentJob.isEmpty()) {
      submittedJob = executorService.submit(this::createJob);
    } else {
      Job job = currentJob.get();

      if (requiresUpdate(job)) {
        submittedJob = executorService.submit(() -> updateJob(job));
      } else {
        return updateStatus(job);
      }
    }

    try {
      return submittedJob.get(getJobUpdateTimeoutSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Unable to start job for source {} and sink {}: {}", source, store, e.getMessage());
      return null;
    } finally {
      executorService.shutdownNow();
    }
  }

  boolean requiresUpdate(Job job) {
    // If set of feature sets has changed
    if (!Sets.newHashSet(featureSets).equals(Sets.newHashSet(job.getFeatureSets()))) {
      return true;
    }

    // If any existing feature set populated by the job has its status as pending
    for (FeatureSet featureSet : job.getFeatureSets()) {
      if (featureSet.getStatus().equals(FeatureSetStatus.STATUS_PENDING)) {
        return true;
      }
    }
    return false;
  }

  private Job createJob() {
    String jobId = createJobId(source.getId(), store.getName());
    return startJob(jobId);
  }

  /** Start or update the job to ingest data to the sink. */
  private Job startJob(String jobId) {

    Job job =
        new Job(
            jobId, "", jobManager.getRunnerType(), source, store, featureSets, JobStatus.PENDING);
    try {
      logAudit(Action.SUBMIT, job, "Building graph and submitting to %s", runnerName);

      job = jobManager.startJob(job);
      var extId = job.getExtId();
      if (extId.isEmpty()) {
        throw new RuntimeException(
            String.format("Could not submit job: \n%s", "unable to retrieve job external id"));
      }

      var auditMessage = "Job submitted to runner %s with ext id %s.";
      logAudit(Action.STATUS_CHANGE, job, auditMessage, runnerName, extId);

      return job;
    } catch (Exception e) {
      log.error(e.getMessage());
      var auditMessage = "Job failed to be submitted to runner %s. Job status changed to ERROR.";
      logAudit(Action.STATUS_CHANGE, job, auditMessage, runnerName);

      job.setStatus(JobStatus.ERROR);
      return job;
    }
  }

  /** Update the given job */
  private Job updateJob(Job job) {
    job.setFeatureSets(featureSets);
    job.setStore(store);
    logAudit(Action.UPDATE, job, "Updating job %s for runner %s", job.getId(), runnerName);
    return jobManager.updateJob(job);
  }

  private Job updateStatus(Job job) {
    JobStatus currentStatus = job.getStatus();
    JobStatus newStatus = jobManager.getJobStatus(job);
    if (newStatus != currentStatus) {
      var auditMessage = "Job status updated: changed from %s to %s";
      logAudit(Action.STATUS_CHANGE, job, auditMessage, currentStatus, newStatus);
    }

    job.setStatus(newStatus);
    return job;
  }

  String createJobId(String sourceId, String storeName) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String sourceIdTrunc = sourceId.split("/")[0].toLowerCase();
    String jobId = String.format("%s-to-%s", sourceIdTrunc, storeName) + dateSuffix;
    return jobId.replaceAll("_", "-");
  }

  private void logAudit(Action action, Job job, String detail, Object... args) {
    AuditLogger.log(Resource.JOB, job.getId(), action, detail, args);
  }
}
