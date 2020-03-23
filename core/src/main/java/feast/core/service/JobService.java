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
import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Defines a Job Managemenent Service that allows users to manage feast ingestion jobs. */
@Service
public class JobService {
  private JobRepository jobRepository;
  private SpecService specService;
  private Map<String, JobManager> jobManagers;

  @Autowired
  public JobService(
      JobRepository jobRepository, SpecService specService, List<JobManager> jobManagerList) {
    this.jobRepository = jobRepository;
    this.specService = specService;

    this.jobManagers = new HashMap<>();
    for (JobManager manager : jobManagerList) {
      this.jobManagers.put(manager.getRunnerType().getName(), manager);
    }
  }

  /* Job Service API */
  /**
   * List Ingestion Jobs in feast matching the given request
   *
   * @param request list ingestion jobs request specifying which jobs to include
   * @throws InvalidArgumentException when given filter in a unsupported configuration
   * @throws InvalidProtocolBufferException on error when constructing response protobuf
   * @return list ingestion jobs response
   */
  public ListIngestionJobsResponse listJobs(ListIngestionJobsRequest request)
      throws InvalidProtocolBufferException {
    // filter jobs based on request filter
    ListIngestionJobsRequest.Filter filter = request.getFilter();
    Set<String> matchingJobIds = new HashSet<>();

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
        List<String> jobIds =
            jobs.stream()
                .map(
                    job -> {
                      return job.getId();
                    })
                .collect(Collectors.toList());
        matchingJobIds = this.mergeResults(matchingJobIds, jobIds);
      }
      if (filter.hasFeatureSetReference()) {
        // find a matching featuresets for reference
        FeatureSetReference fsReference = filter.getFeatureSetReference();
        ListFeatureSetsResponse response = this.specService.matchFeatureSets(fsReference);
        List<FeatureSet> featureSets =
            response.getFeatureSetsList().stream()
                .map(
                    fsProto -> {
                      return FeatureSet.fromProto(fsProto);
                    })
                .collect(Collectors.toList());

        // find jobs for the matching featuresets
        Collection<Job> matchingJobs = this.jobRepository.findByFeatureSetIn(featureSets);
        List<String> jobIds =
            matchingJobs.stream()
                .map(
                    job -> {
                      return job.getId();
                    })
                .collect(Collectors.toList());
        matchingJobIds = this.mergeResults(matchingJobIds, jobIds);
      }
    }

    // convert matching job models to ingestion job protos
    List<IngestionJobProto.IngestionJob> ingestJobs = new ArrayList<>();
    for (String jobId : matchingJobIds) {
      Job job = this.jobRepository.findById(jobId).get();
      ingestJobs.add(job.toIngestionProto());
    }

    // pack jobs into response
    return ListIngestionJobsResponse.newBuilder().addAllJobs(ingestJobs).build();
  }

  /**
   * Restart (Aborts) the ingestion job matching the given restart request.
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
      throw new NoSuchElementException(
          "Attempted to stop nonexistent job with id: " + getJob.get().getId());
    }

    // check job status is valid for restarting
    Job job = getJob.get();
    JobStatus status = job.getStatus();
    if (JobStatus.getTransitionalStates().contains(status) || status.equals(JobStatus.UNKNOWN)) {
      throw new UnsupportedOperationException(
          "Restarting a job with a transitional or unknown status is unsupported");
    }

    // restart job with job manager
    JobManager jobManager = this.jobManagers.get(job.getRunner());
    job = jobManager.restartJob(job);

    // update job model in job repository
    this.jobRepository.saveAndFlush(job);

    return RestartIngestionJobResponse.newBuilder().build();
  }

  /**
   * Stops (Aborts) the ingestion job matching the given stop request. Does nothing if the target
   * job if already in a terminal states Does not support stopping a job in a transitional or
   * unknown status
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
    if (JobStatus.getTerminalState().contains(status)) {
      // do nothing - job is already stopped
      return StopIngestionJobResponse.newBuilder().build();
    } else if (JobStatus.getTransitionalStates().contains(status)
        || status.equals(JobStatus.UNKNOWN)) {
      throw new UnsupportedOperationException(
          "Stopping a job with a transitional or unknown status is unsupported");
    }

    // stop job with job manager
    JobManager jobManager = this.jobManagers.get(job.getRunner());
    jobManager.abortJob(job.getExtId());

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
}
