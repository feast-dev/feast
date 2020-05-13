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
package feast.core.service;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.CoreServiceProto.ListIngestionJobsRequest;
import feast.core.CoreServiceProto.ListIngestionJobsResponse;
import feast.core.CoreServiceProto.RestartIngestionJobRequest;
import feast.core.CoreServiceProto.RestartIngestionJobResponse;
import feast.core.CoreServiceProto.StopIngestionJobRequest;
import feast.core.CoreServiceProto.StopIngestionJobResponse;
import feast.core.FeatureSetReferenceProto.FeatureSetReference;
import feast.core.IngestionJobProto;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** A Job Management Service that allows users to manage Feast ingestion jobs. */
@Slf4j
@Service
public class JobService {
  private final JobRepository jobRepository;
  private final SpecService specService;
  private final Map<Runner, JobManager> jobManagers;

  @Autowired
  public JobService(
      JobRepository jobRepository, SpecService specService, List<JobManager> jobManagerList) {
    this.jobRepository = jobRepository;
    this.specService = specService;

    this.jobManagers = new HashMap<>();
    for (JobManager manager : jobManagerList) {
      this.jobManagers.put(manager.getRunnerType(), manager);
    }
  }

  /* Job Service API */
  /**
   * List Ingestion Jobs in Feast matching the given request. See CoreService protobuf documentation
   * for more detailed documentation.
   *
   * @param request list ingestion jobs request specifying which jobs to include
   * @throws IllegalArgumentException when given filter in a unsupported configuration
   * @throws InvalidProtocolBufferException on error when constructing response protobuf
   * @return list ingestion jobs response
   */
  @Transactional(readOnly = true)
  public ListIngestionJobsResponse listJobs(ListIngestionJobsRequest request)
      throws InvalidProtocolBufferException {
    Set<String> matchingJobIds = new HashSet<>();

    // check that filter specified and not empty
    if (request.hasFilter()
        && !(request.getFilter().getId() == ""
            && request.getFilter().getStoreName() == ""
            && request.getFilter().hasFeatureSetReference() == false)) {
      // filter jobs based on request filter
      ListIngestionJobsRequest.Filter filter = request.getFilter();

      // for proto3, default value for missing values:
      // - numeric values (ie int) is zero
      // - strings is empty string
      if (filter.getId() != "") {
        // get by id: no more filters required: found job
        Optional<Job> job = this.jobRepository.findById(filter.getId());
        if (job.isPresent()) {
          matchingJobIds.add(filter.getId());
        }
      } else {
        // multiple filters can apply together in an 'and' operation
        if (filter.getStoreName() != "") {
          // find jobs by name
          List<Job> jobs = this.jobRepository.findByStoreName(filter.getStoreName());
          Set<String> jobIds = jobs.stream().map(Job::getId).collect(Collectors.toSet());
          matchingJobIds = this.mergeResults(matchingJobIds, jobIds);
        }
        if (filter.hasFeatureSetReference()) {
          // find a matching featuresets for reference
          FeatureSetReference fsReference = filter.getFeatureSetReference();
          ListFeatureSetsResponse response =
              this.specService.listFeatureSets(this.toListFeatureSetFilter(fsReference));
          List<FeatureSet> featureSets =
              response.getFeatureSetsList().stream()
                  .map(FeatureSet::fromProto)
                  .collect(Collectors.toList());

          // find jobs for the matching featuresets
          Collection<Job> matchingJobs = this.jobRepository.findByFeatureSetsIn(featureSets);
          List<String> jobIds = matchingJobs.stream().map(Job::getId).collect(Collectors.toList());
          matchingJobIds = this.mergeResults(matchingJobIds, jobIds);
        }
      }
    } else {
      // no or empty filter: match all jobs
      matchingJobIds =
          this.jobRepository.findAll().stream().map(Job::getId).collect(Collectors.toSet());
    }

    // convert matching job models to ingestion job protos
    List<IngestionJobProto.IngestionJob> ingestJobs = new ArrayList<>();
    for (String jobId : matchingJobIds) {
      Job job = this.jobRepository.findById(jobId).get();
      ingestJobs.add(job.toProto());
    }

    // pack jobs into response
    return ListIngestionJobsResponse.newBuilder().addAllJobs(ingestJobs).build();
  }

  /**
   * Restart (Aborts) the ingestion job matching the given restart request. See CoreService protobuf
   * documentation for more detailed documentation.
   *
   * @param request restart ingestion job request specifying which job to stop
   * @throws NoSuchElementException when restart job request requests to restart a nonexistent job.
   * @throws UnsupportedOperationException when job to be restarted is in an unsupported status
   * @throws InvalidProtocolBufferException on error when constructing response protobuf
   */
  @Transactional
  public RestartIngestionJobResponse restartJob(RestartIngestionJobRequest request)
      throws InvalidProtocolBufferException {
    // check job exists
    Optional<Job> getJob = this.jobRepository.findById(request.getId());
    if (getJob.isEmpty()) {
      // FIXME: if getJob.isEmpty then constructing this error message will always throw an error...
      throw new NoSuchElementException(
          "Attempted to stop nonexistent job with id: " + getJob.get().getId());
    }

    // check job status is valid for restarting
    Job job = getJob.get();
    JobStatus status = job.getStatus();
    if (status.isTransitional() || status.isTerminal() || status == JobStatus.UNKNOWN) {
      throw new UnsupportedOperationException(
          "Restarting a job with a transitional, terminal or unknown status is unsupported");
    }

    // restart job with job manager
    JobManager jobManager = this.jobManagers.get(job.getRunner());
    job = jobManager.restartJob(job);
    log.info(
        String.format(
            "Restarted job (id: %s, extId: %s runner: %s)",
            job.getId(), job.getExtId(), job.getRunner()));
    // sync job status & update job model in job repository
    job = this.syncJobStatus(jobManager, job);
    this.jobRepository.saveAndFlush(job);

    return RestartIngestionJobResponse.newBuilder().build();
  }

  /**
   * Stops (Aborts) the ingestion job matching the given stop request. See CoreService protobuf
   * documentation for more detailed documentation.
   *
   * @param request stop ingestion job request specifying which job to stop
   * @throws NoSuchElementException when stop job request requests to stop a nonexistent job.
   * @throws UnsupportedOperationException when job to be stopped is in an unsupported status
   * @throws InvalidProtocolBufferException on error when constructing response protobuf
   */
  @Transactional
  public StopIngestionJobResponse stopJob(StopIngestionJobRequest request)
      throws InvalidProtocolBufferException {
    // check job exists
    Optional<Job> getJob = this.jobRepository.findById(request.getId());
    if (getJob.isEmpty()) {
      throw new NoSuchElementException(
          "Attempted to stop nonexistent job with id: " + getJob.get().getId());
    }

    // check job status is valid for stopping
    Job job = getJob.get();
    JobStatus status = job.getStatus();
    if (status.isTerminal()) {
      // do nothing - job is already stopped
      return StopIngestionJobResponse.newBuilder().build();
    } else if (status.isTransitional() || status == JobStatus.UNKNOWN) {
      throw new UnsupportedOperationException(
          "Stopping a job with a transitional or unknown status is unsupported");
    }

    // stop job with job manager
    JobManager jobManager = this.jobManagers.get(job.getRunner());
    jobManager.abortJob(job.getExtId());
    log.info(
        String.format(
            "Restarted job (id: %s, extId: %s runner: %s)",
            job.getId(), job.getExtId(), job.getRunner()));

    // sync job status & update job model in job repository
    job = this.syncJobStatus(jobManager, job);
    this.jobRepository.saveAndFlush(job);

    return StopIngestionJobResponse.newBuilder().build();
  }

  /* Private Utility Methods */
  private <T> Set<T> mergeResults(Set<T> results, Collection<T> newResults) {
    if (results.size() <= 0) {
      // no existing results: copy over new results
      results.addAll(newResults);
    } else {
      // and operation: keep results that exist in both existing and new results
      results.retainAll(newResults);
    }
    return results;
  }

  // converts feature set reference to a list feature set filter
  private ListFeatureSetsRequest.Filter toListFeatureSetFilter(FeatureSetReference fsReference) {
    // match featuresets using contents of featureset reference
    String fsName = fsReference.getName();
    String fsProject = fsReference.getProject();

    // construct list featureset request filter using feature set reference
    // for proto3, default value for missing values:
    // - numeric values (ie int) is zero
    // - strings is empty string
    ListFeatureSetsRequest.Filter filter =
        ListFeatureSetsRequest.Filter.newBuilder()
            .setFeatureSetName((fsName != "") ? fsName : "*")
            .setProject((fsProject != "") ? fsProject : "*")
            .build();

    return filter;
  }

  // sync job status using job manager
  private Job syncJobStatus(JobManager jobManager, Job job) {
    JobStatus newStatus = jobManager.getJobStatus(job);
    // log job status transition
    if (newStatus != job.getStatus()) {
      AuditLogger.log(
          Resource.JOB,
          job.getId(),
          Action.STATUS_CHANGE,
          "Job status transition: changed from %s to %s",
          job.getStatus(),
          newStatus);
      job.setStatus(newStatus);
    }
    return job;
  }
}
