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
import feast.core.CoreServiceProto.ListIngestionJobsRequest;
import feast.core.CoreServiceProto.ListIngestionJobsResponse;
import feast.core.CoreServiceProto.StopIngestionJobRequest;
import feast.core.CoreServiceProto.StopIngestionJobResponse;
import feast.core.FeatureSetReferenceProto.FeatureSetReference;
import feast.core.IngestionJobProto;
import feast.core.dao.FeatureSetRepository;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Defines a Job Managemenent Service that allows users to manage feast ingestion jobs. */
@Service
public class JobService {
  private JobRepository jobRepository;
  private FeatureSetRepository featureSetRepository;
  private Map<String, JobManager> jobManagers;

  @Autowired
  public JobService(
      JobRepository jobRepository,
      FeatureSetRepository featureSetRepository,
      List<JobManager> jobManagerList) {
    this.jobRepository = jobRepository;
    this.featureSetRepository = featureSetRepository;

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
   * @throws UnsupportedOperationException when given filter in a unsupported configuration
   * @throws InvalidProtocolBufferException on error when constructing response protobuf
   * @return list ingestion jobs response
   */
  public ListIngestionJobsResponse listJobs(ListIngestionJobsRequest request)
      throws UnsupportedOperationException, InvalidProtocolBufferException {
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
        // find a matching featureset for reference
        FeatureSetReference fsReference = filter.getFeatureSetReference();
        List<FeatureSet> matchFeatureSets = this.findFeatureSets(fsReference);
        Collection<Job> jobs = this.jobRepository.findByFeatureSetIn(matchFeatureSets);

        List<String> jobIds =
            jobs.stream()
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

  // TODO: restart ingestion job

  /**
   * Stops (Aborts) the ingestion job matching the given request. 
   * Does nothing if the target job if already in a terminal states
   * Errors when attempting to stop a job in a transitional or unknown job
   *
   * @param request stop ingestion job request specifying which job to stop
   * @throws NoSuchElementException when stop job request requests to stop a nonexistent job.
   * @throws UnsupportedOperationException when job to be stopped is in an unsupported status
   * @throws InvalidProtocolBufferException on error when constructing response protobuf
   */
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
    if(JobStatus.getTerminalState().contains(status)) {
      // do nothing - job is already stoped
      return StopIngestionJobResponse.newBuilder().build();
    } else if (JobStatus.getTransitionalStates().contains(status) ||
        status.equals(JobStatus.UNKNOWN)) {
      throw new UnsupportedOperationException(
          "Stopping a job with an unknown status is unsupported");
    }

    // stop job with job manager
    JobManager jobManager = this.jobManagers.get(job.getRunner());
    jobManager.abortJob(job.getExtId());

    return StopIngestionJobResponse.newBuilder().build();
  }

  /* Private Utility Methods */
  /**
   * Finds &amp; returns featuresets matching the given feature set refererence
   *
   * @param fsReference FeatureSetReference that specifies which featuresets to match
   * @throws UnsupportedOperationException fsReference given is unsupported.
   * @return Returns a list of matching featuresets
   */
  private List<FeatureSet> findFeatureSets(FeatureSetReference fsReference)
      throws UnsupportedOperationException {

    String fsName = fsReference.getName();
    String fsProject = fsReference.getProject();
    Integer fsVersion = fsReference.getVersion();

    List<FeatureSet> featureSets = new ArrayList<>();
    if (fsName != "" && fsProject != "" && fsVersion != 0) {
      featureSets.add(
          this.featureSetRepository.findFeatureSetByNameAndProject_NameAndVersion(
              fsName, fsProject, fsVersion));
    } else if (fsName != "" && fsProject != "") {
      featureSets.addAll(this.featureSetRepository.findAllByNameAndProject_Name(fsName, fsProject));
    } else if (fsName != "" && fsVersion != 0) {
      featureSets.addAll(this.featureSetRepository.findAllByNameAndVersion(fsName, fsVersion));
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported featureset refererence configuration: "
                  + "(name: '%s', project: '%s', version: '%d')",
              fsName, fsProject, fsVersion));
    }

    return featureSets;
  }

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
