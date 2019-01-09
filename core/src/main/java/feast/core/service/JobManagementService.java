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
import feast.core.config.ImportJobDefaults;
import feast.core.dao.JobInfoRepository;
import feast.core.dao.MetricsRepository;
import feast.core.exception.JobExecutionException;
import feast.core.exception.RetrievalException;
import feast.core.job.JobManager;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import feast.specs.ImportSpecProto.ImportSpec;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class JobManagementService {
  private static final String JOB_PREFIX_DEFAULT = "feastimport";

  private JobInfoRepository jobInfoRepository;
  private MetricsRepository metricsRepository;
  private JobManager jobManager;
  private ImportJobDefaults defaults;

  @Autowired
  public JobManagementService(
      JobInfoRepository jobInfoRepository,
      MetricsRepository metricsRepository,
      JobManager jobManager,
      ImportJobDefaults defaults) {
    this.jobInfoRepository = jobInfoRepository;
    this.metricsRepository = metricsRepository;
    this.jobManager = jobManager;
    this.defaults = defaults;
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
   * Submit ingestion job to runner.
   *
   * @param importSpec import spec of the ingestion job
   * @param namePrefix name prefix of the ingestion job
   * @return feast job ID.
   */
  public String submitJob(ImportSpec importSpec, String namePrefix) {
    String feastJobId = createJobId(namePrefix);
    try {
      JobInfo jobInfo =
          new JobInfo(feastJobId, "", defaults.getRunner(), importSpec, JobStatus.PENDING);
      jobInfoRepository.saveAndFlush(jobInfo);
      AuditLogger.log(
          Resource.JOB,
          feastJobId,
          Action.SUBMIT,
          "Building graph and submitting to %s",
          defaults.getRunner());

      String extId = jobManager.submitJob(importSpec, namePrefix);

      AuditLogger.log(
          Resource.JOB,
          feastJobId,
          Action.STATUS_CHANGE,
          "Job submitted to runner %s with ext id %s.",
          defaults.getRunner(),
          extId);
      return feastJobId;
    } catch (Exception e) {
      updateJobStatus(feastJobId, JobStatus.ERROR);
      AuditLogger.log(
          Resource.JOB,
          feastJobId,
          Action.STATUS_CHANGE,
          "Job failed to be submitted to runner %s. Job status changed to ERROR.",
          defaults.getRunner());
      throw new JobExecutionException(String.format("Error running ingestion job: %s", e), e);
    }
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

  /**
   * Update a given job's status
   *
   * @param jobId
   * @param status
   */
  private void updateJobStatus(String jobId, JobStatus status) {
    Optional<JobInfo> jobRecordOptional = jobInfoRepository.findById(jobId);
    if (jobRecordOptional.isPresent()) {
      JobInfo jobRecord = jobRecordOptional.get();
      jobRecord.setStatus(status);
      jobInfoRepository.saveAndFlush(jobRecord);
    }
  }


  private String createJobId(String namePrefix) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    return namePrefix.isEmpty() ? JOB_PREFIX_DEFAULT + dateSuffix : namePrefix + dateSuffix;
  }
}
