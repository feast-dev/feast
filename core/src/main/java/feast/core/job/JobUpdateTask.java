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
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.*;
import feast.proto.core.FeatureSetProto;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
 * their source and sink to transition to targetStatus.
 *
 * <p>When complete, the JobUpdateTask returns the updated Job object to be pushed to the db.
 */
@Slf4j
@Getter
public class JobUpdateTask implements Callable<Job> {

  /**
   * JobTargetStatus enum defines the possible target statuses that JobUpdateTask can transition a
   * Job to.
   */
  public enum JobTargetStatus {
    RUNNING,
    ABORTED
  }

  private final List<FeatureSet> featureSets;
  private final Source source;
  private final Store store;
  private final JobTargetStatus targetStatus;
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
      long jobUpdateTimeoutSeconds,
      JobTargetStatus targetStatus) {
    this.featureSets = featureSets;
    this.source = source;
    this.store = store;
    this.currentJob = currentJob;
    this.jobManager = jobManager;
    this.jobUpdateTimeoutSeconds = jobUpdateTimeoutSeconds;
    this.runnerName = jobManager.getRunnerType().toString();
    this.targetStatus = targetStatus;
  }

  @Override
  public Job call() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Job> submittedJob;

    if (this.targetStatus.equals(JobTargetStatus.RUNNING) && currentJob.isEmpty()) {
      submittedJob = executorService.submit(this::createJob);
    } else if (this.targetStatus.equals(JobTargetStatus.RUNNING)
        && currentJob.isPresent()
        && requiresUpdate(currentJob.get())) {
      submittedJob = executorService.submit(() -> updateJob(currentJob.get()));
    } else if (this.targetStatus.equals(JobTargetStatus.ABORTED)
        && currentJob.isPresent()
        && currentJob.get().getStatus() == JobStatus.RUNNING) {
      submittedJob = executorService.submit(() -> stopJob(currentJob.get()));
    } else if (this.targetStatus.equals(JobTargetStatus.ABORTED) && currentJob.isEmpty()) {
      throw new IllegalArgumentException("Cannot abort an nonexistent ingestion job.");
    } else {
      return this.updateStatus(currentJob.get());
    }

    try {
      return submittedJob.get(getJobUpdateTimeoutSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("Unable to start job for source {} and sink {}:", source, store);
      e.printStackTrace();
      return null;
    } finally {
      executorService.shutdownNow();
    }
  }

  boolean requiresUpdate(Job job) {
    // if store subscriptions have changed
    if (!Sets.newHashSet(store.getSubscriptions())
        .equals(Sets.newHashSet(job.getStore().getSubscriptions()))) {
      return true;
    }

    return false;
  }

  private Job createJob() {
    String jobId = createJobId(source, store.getName());
    return startJob(jobId);
  }

  /** Start or update the job to ingest data to the sink. */
  private Job startJob(String jobId) {
    Job job =
        Job.builder()
            .setId(jobId)
            .setRunner(jobManager.getRunnerType())
            .setSource(source)
            .setStore(store)
            .setStatus(JobStatus.PENDING)
            .setFeatureSetJobStatuses(new HashSet<>())
            .build();

    updateFeatureSets(job);

    try {
      logAudit(Action.SUBMIT, job, "Building graph and submitting to %s", runnerName);

      System.out.println(
          job.equals(
              Job.builder()
                  .setId("job")
                  .setExtId("")
                  .setRunner(Runner.DATAFLOW)
                  .setSource(source)
                  .setStore(store)
                  .setStatus(JobStatus.PENDING)
                  .build()));

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

  private void updateFeatureSets(Job job) {
    Map<FeatureSet, FeatureSetJobStatus> alreadyConnected =
        job.getFeatureSetJobStatuses().stream()
            .collect(Collectors.toMap(FeatureSetJobStatus::getFeatureSet, s -> s));

    for (FeatureSet fs : featureSets) {
      if (alreadyConnected.containsKey(fs)) {
        continue;
      }

      FeatureSetJobStatus status = new FeatureSetJobStatus();
      status.setFeatureSet(fs);
      status.setJob(job);
      if (fs.getStatus() == FeatureSetProto.FeatureSetStatus.STATUS_READY) {
        // Feature Set was already delivered to previous generation of the job
        // (another words, it exists in kafka)
        // so we expect Job will ack latest version based on history from kafka topic
        status.setVersion(fs.getVersion());
      }
      status.setDeliveryStatus(FeatureSetProto.FeatureSetJobDeliveryStatus.STATUS_IN_PROGRESS);
      job.getFeatureSetJobStatuses().add(status);
    }
  }

  /** Update the given job */
  private Job updateJob(Job job) {
    updateFeatureSets(job);
    job.setStore(store);
    logAudit(Action.UPDATE, job, "Updating job %s for runner %s", job.getId(), runnerName);
    return jobManager.updateJob(job);
  }

  /** Stop the given job */
  private Job stopJob(Job job) {
    logAudit(Action.ABORT, job, "Aborting job %s for runner %s", job.getId(), runnerName);
    return jobManager.abortJob(job);
  }

  private Job updateStatus(Job job) {
    JobStatus currentStatus = job.getStatus();
    JobStatus newStatus = jobManager.getJobStatus(job);
    if (newStatus != currentStatus) {
      var auditMessage = "Job status updated: changed from %s to %s";
      logAudit(Action.STATUS_CHANGE, job, auditMessage, currentStatus, newStatus);
    }

    job.setStatus(newStatus);
    updateFeatureSets(job);
    return job;
  }

  String createJobId(Source source, String storeName) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    String jobId =
        String.format(
            "%s-%d-to-%s-%s",
            source.getTypeString(), Objects.hashCode(source.getConfig()), storeName, dateSuffix);
    return jobId.replaceAll("_store", "-").toLowerCase();
  }

  private void logAudit(Action action, Job job, String detail, Object... args) {
    AuditLogger.log(Resource.JOB, job.getId(), action, detail, args);
  }
}
