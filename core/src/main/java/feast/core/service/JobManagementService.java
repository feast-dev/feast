/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.service;

import com.google.common.base.Strings;
import feast.core.JobServiceProto.JobServiceTypes.JobDetail;
import feast.core.dao.JobInfoRepository;
import feast.core.dao.MetricsRepository;
import feast.core.exception.RetrievalException;
import feast.core.job.JobManager;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
public class JobManagementService {
  @Autowired private JobInfoRepository jobInfoRepository;
  @Autowired private MetricsRepository metricsRepository;
  @Autowired private JobManager jobManager;

  public JobManagementService(
      JobInfoRepository jobInfoRepository,
      MetricsRepository metricsRepository,
      JobManager jobManager) {
    this.jobInfoRepository = jobInfoRepository;
    this.metricsRepository = metricsRepository;
    this.jobManager = jobManager;
  }

  /**
   * Lists all jobs registered to the db.
   *
   * @return list of JobDetails
   */
  @Transactional
  public List<JobDetail> listJobs() {
    List<JobInfo> jobs = jobInfoRepository.findAll();
    return jobs.stream().map(JobInfo::getJobDetail).collect(Collectors.toList());
  }

  /**
   * Gets information regarding a single job.
   *
   * @param id feast-internal job id
   * @return JobDetail for that job
   */
  @Transactional
  public JobDetail getJob(String id) {
    Optional<JobInfo> job = jobInfoRepository.findById(id);
    if (!job.isPresent()) {
      throw new RetrievalException(Strings.lenientFormat("Unable to retrieve job with id %s", id));
    }
    JobDetail.Builder jobDetailBuilder = job.get().getJobDetail().toBuilder();
    List<Metrics> metrics = metricsRepository.findByJobInfo_Id(id);
    for (Metrics metric : metrics) {
      jobDetailBuilder.putMetrics(metric.getName(), metric.getValue());
    }
    return jobDetailBuilder.build();
  }

  /**
   * Drain the given job. If this is successful, the job will start the draining process. When the
   * draining process is complete, the job will be cleaned up and removed.
   *
   * <p>Batch jobs will be cancelled, as draining these jobs is not supported by beam.
   *
   * @param id feast-internal id of a job
   */
  public void abortJob(String id) {
    Optional<JobInfo> jobOptional = jobInfoRepository.findById(id);
    if (!jobOptional.isPresent()) {
      throw new RetrievalException(Strings.lenientFormat("Unable to retrieve job with id %s", id));
    }
    JobInfo job = jobOptional.get();
    if (JobStatus.getTerminalState().contains(job.getStatus())) {
      throw new IllegalStateException("Unable to stop job already in terminal state");
    }
    jobManager.abortJob(job.getExtId());
    job.setStatus(JobStatus.ABORTING);

    AuditLogger.log(Resource.JOB, id, Action.ABORT, "Triggering draining of job");
    jobInfoRepository.saveAndFlush(job);
  }
}
