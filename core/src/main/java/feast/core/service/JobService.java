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

import feast.common.models.FeatureSetReference;
import feast.core.job.JobManager;
import feast.core.job.JobRepository;
import feast.core.job.task.RestartJobTask;
import feast.core.job.task.TerminateJobTask;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.proto.core.CoreServiceProto.ListIngestionJobsRequest;
import feast.proto.core.CoreServiceProto.ListIngestionJobsResponse;
import feast.proto.core.CoreServiceProto.RestartIngestionJobRequest;
import feast.proto.core.CoreServiceProto.RestartIngestionJobResponse;
import feast.proto.core.CoreServiceProto.StopIngestionJobRequest;
import feast.proto.core.CoreServiceProto.StopIngestionJobResponse;
import feast.proto.core.FeatureSetReferenceProto;
import feast.proto.core.IngestionJobProto;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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
  private final JobManager jobManager;

  @Autowired
  public JobService(JobRepository jobRepository, JobManager jobManager) {
    this.jobRepository = jobRepository;
    this.jobManager = jobManager;
  }

  // region Job Service API

  /**
   * List Ingestion Jobs in Feast matching the given request. See CoreService protobuf documentation
   * for more detailed documentation.
   *
   * @param request list ingestion jobs request specifying which jobs to include
   * @throws IllegalArgumentException when given filter in a unsupported configuration
   * @return list ingestion jobs response
   */
  @Transactional(readOnly = true)
  public ListIngestionJobsResponse listJobs(ListIngestionJobsRequest request) {
    Set<String> matchingJobIds = new HashSet<>();

    // check that filter specified and not empty
    if (request.hasFilter()
        && !(request.getFilter().getId().isEmpty()
            && request.getFilter().getStoreName().isEmpty()
            && !request.getFilter().hasFeatureSetReference())) {
      // filter jobs based on request filter
      ListIngestionJobsRequest.Filter filter = request.getFilter();

      // for proto3, default value for missing values:
      // - numeric values (ie int) is zero
      // - strings is empty string
      if (!filter.getId().isEmpty()) {
        // get by id: no more filters required: found job
        Optional<Job> job = this.jobRepository.findById(filter.getId());
        if (job.isPresent()) {
          matchingJobIds.add(filter.getId());
        }
      } else {
        // multiple filters can apply together in an 'and' operation
        if (!filter.getStoreName().isEmpty()) {
          // find jobs by name
          List<Job> jobs = this.jobRepository.findByJobStoreName(filter.getStoreName());
          Set<String> jobIds = jobs.stream().map(Job::getId).collect(Collectors.toSet());
          matchingJobIds = this.mergeResults(matchingJobIds, jobIds);
        }
        if (filter.hasFeatureSetReference()) {
          // find a matching featuresets for reference
          FeatureSetReferenceProto.FeatureSetReference fsReference =
              filter.getFeatureSetReference();

          // find jobs for the matching featuresets
          Collection<Job> matchingJobs =
              this.jobRepository.findByFeatureSetReference(
                  FeatureSetReference.of(fsReference.getProject(), fsReference.getName()));
          List<String> jobIds =
              matchingJobs.stream()
                  .filter(job -> job.getStatus().equals(JobStatus.RUNNING))
                  .map(Job::getId)
                  .collect(Collectors.toList());
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
      Job job = this.jobRepository.findById(jobId).orElseThrow();
      // job that failed on start won't be converted toProto successfully
      // and they're irrelevant here
      if (job.getStatus() == JobStatus.ERROR) {
        continue;
      }
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
   */
  @Transactional
  public RestartIngestionJobResponse restartJob(RestartIngestionJobRequest request) {
    String jobId = request.getId();

    Job job =
        this.jobRepository
            .findById(jobId)
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "Attempted to restart nonexistent job with id: " + jobId));

    // check job status is valid for restarting
    JobStatus status = job.getStatus();
    if (status.isTransitional() || status.isTerminal() || status == JobStatus.UNKNOWN) {
      throw new UnsupportedOperationException(
          "Restarting a job with a transitional, terminal or unknown status is unsupported");
    }

    // restart job by running job task
    new RestartJobTask(job, this.jobManager).call();

    // update job model in job repository
    this.jobRepository.add(job);

    return RestartIngestionJobResponse.newBuilder().build();
  }

  /**
   * Stops (Aborts) the ingestion job matching the given stop request. See CoreService protobuf
   * documentation for more detailed documentation.
   *
   * @param request stop ingestion job request specifying which job to stop
   * @throws NoSuchElementException when stop job request requests to stop a nonexistent job.
   * @throws UnsupportedOperationException when job to be stopped is in an unsupported status
   */
  @Transactional
  public StopIngestionJobResponse stopJob(StopIngestionJobRequest request) {
    String jobId = request.getId();

    Job job =
        this.jobRepository
            .findById(jobId)
            .orElseThrow(
                () ->
                    new NoSuchElementException(
                        "Attempted to stop nonexistent job with id: " + jobId));

    // check job status is valid for stopping
    JobStatus status = job.getStatus();
    if (status.isTerminal()) {
      // do nothing - job is already stopped
      return StopIngestionJobResponse.newBuilder().build();
    } else if (status.isTransitional() || status == JobStatus.UNKNOWN) {
      throw new UnsupportedOperationException(
          "Stopping a job with a transitional or unknown status is unsupported");
    }

    // stop job with job task
    new TerminateJobTask(job, this.jobManager).call();

    // update job model in job repository
    this.jobRepository.add(job);

    return StopIngestionJobResponse.newBuilder().build();
  }

  // endregion
  // region Private Utility Methods

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
}
