package feast.core.service;

import com.google.common.base.Strings;
import feast.core.config.ImportJobDefaults;
import feast.core.dao.JobInfoRepository;
import feast.core.exception.JobExecutionException;
import feast.core.exception.RetrievalException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.log.Action;
import feast.core.log.AuditLogger;
import feast.core.log.Resource;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.util.PathUtil;
import feast.specs.ImportSpecProto.ImportSpec;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class JobCoordinatorService {
  private static final String JOB_PREFIX_DEFAULT = "feastimport";
  private static final String UNKNOWN_EXT_JOB_ID = "";

  private JobInfoRepository jobInfoRepository;
  private JobManager jobManager;
  private ImportJobDefaults defaults;

  @Autowired
  public JobCoordinatorService(
      JobInfoRepository jobInfoRepository,
      JobManager jobManager,
      ImportJobDefaults defaults) {
    this.jobInfoRepository = jobInfoRepository;
    this.jobManager = jobManager;
    this.defaults = defaults;
  }

  /**
   * Submit ingestion job to runner.
   *
   * @param importSpec import spec of the ingestion job
   * @param namePrefix name prefix of the ingestion job
   * @return feast job ID.
   */
  public String startJob(ImportSpec importSpec, String namePrefix) {
    String jobId = createJobId(namePrefix);
    Path workspace = PathUtil.getPath(defaults.getWorkspace()).resolve(jobId);
    try {
      Files.createDirectory(workspace);
    } catch (FileAlreadyExistsException e) {
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Could not initialise job workspace job: %s", workspace.toString()), e);
    }

    // TODO
    //    ImportJobSpecs importJobSpecs = buildImportJobSpecs(importSpec, jobId);
    //    writeImportJobSpecs(importJobSpecs, workspace);

    boolean isDirectRunner = Runner.DIRECT.getName().equals(defaults.getRunner());
    try {
      if (!isDirectRunner) {
        JobInfo jobInfo =
            new JobInfo(jobId, UNKNOWN_EXT_JOB_ID, defaults.getRunner(), importSpec,
                JobStatus.PENDING);
        jobInfoRepository.save(jobInfo);
      }

      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.SUBMIT,
          "Building graph and submitting to %s",
          defaults.getRunner());

      String extId = jobManager.startJob(jobId, workspace);
      if (extId.isEmpty()) {
        throw new RuntimeException(
            String.format("Could not submit job: \n%s", "unable to retrieve job external id"));
      }

      AuditLogger.log(
          Resource.JOB,
          jobId,
          Action.STATUS_CHANGE,
          "Job submitted to runner %s with ext id %s.",
          defaults.getRunner(),
          extId);

      if (isDirectRunner) {
        JobInfo jobInfo =
            new JobInfo(jobId, extId, defaults.getRunner(), importSpec, JobStatus.COMPLETED);
        jobInfoRepository.save(jobInfo);
      } else {
        updateJobExtId(jobId, extId);
      }
      return jobId;
    } catch (Exception e) {
      updateJobStatus(jobId, JobStatus.ERROR);
      AuditLogger.log(
          Resource.JOB,
          jobId,
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
   */
  void updateJobStatus(String jobId, JobStatus status) {
    Optional<JobInfo> jobRecordOptional = jobInfoRepository.findById(jobId);
    if (jobRecordOptional.isPresent()) {
      JobInfo jobRecord = jobRecordOptional.get();
      jobRecord.setStatus(status);
      jobInfoRepository.save(jobRecord);
    }
  }

  /**
   * Update a given job's external id
   */
  void updateJobExtId(String jobId, String jobExtId) {
    Optional<JobInfo> jobRecordOptional = jobInfoRepository.findById(jobId);
    if (jobRecordOptional.isPresent()) {
      JobInfo jobRecord = jobRecordOptional.get();
      jobRecord.setExtId(jobExtId);
      jobInfoRepository.save(jobRecord);
    }
  }

  private String createJobId(String namePrefix) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    return namePrefix.isEmpty() ? JOB_PREFIX_DEFAULT + dateSuffix : namePrefix + dateSuffix;
  }

  public String getWorkspace() {
    return defaults.getWorkspace();
  }
}
