package feast.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
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
import feast.core.stream.FeatureStream;
import feast.core.util.PathUtil;
import feast.ingestion.util.ProtoUtil;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs.Builder;
import feast.specs.ImportJobSpecsProto.SourceSpec;
import feast.specs.ImportJobSpecsProto.SourceSpec.SourceType;
import feast.specs.StorageSpecProto.StorageSpec;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class JobCoordinatorService {

  private static final String JOB_PREFIX_DEFAULT = "feastimport";
  private static final String UNKNOWN_EXT_JOB_ID = "";
  private static final String IMPORT_JOB_SPECS_FILENAME = "importJobSpecs.yaml";

  private JobInfoRepository jobInfoRepository;
  private JobManager jobManager;
  private FeatureStream featureStream;
  private ImportJobDefaults defaults;

  @Autowired
  public JobCoordinatorService(
      JobInfoRepository jobInfoRepository,
      JobManager jobManager,
      FeatureStream featureStream,
      ImportJobDefaults defaults) {
    this.jobInfoRepository = jobInfoRepository;
    this.jobManager = jobManager;
    this.featureStream = featureStream;
    this.defaults = defaults;
  }

  /**
   * Submit ingestion job to runner.
   *
   * @param importJobSpec import job spec of the ingestion job
   * @return JobInfo of job started
   */
  public JobInfo startJob(ImportJobSpecs importJobSpec) {
    String jobId = importJobSpec.getJobId();
    Path workspace = PathUtil.getPath(defaults.getWorkspace()).resolve(jobId);
    try {
      Files.createDirectory(workspace);
    } catch (FileAlreadyExistsException e) {
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Could not initialise job workspace job: %s", workspace.toString()), e);
    }
    writeImportJobSpecs(importJobSpec, workspace);

    try {
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
      JobInfo jobInfo = new JobInfo(jobId, extId, defaults.getRunner(), importJobSpec,
          JobStatus.RUNNING);
      jobInfoRepository.save(jobInfo);
      return jobInfo;
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
   * Update the given job
   */
  public void updateJob(JobInfo jobInfo, ImportJobSpecs importJobSpecs) {
    ImportJobSpecs existingImportJobSpec = ImportJobSpecs.newBuilder().build();
    try {
      existingImportJobSpec = ProtoUtil.decodeProtoYaml(jobInfo.getRaw(), existingImportJobSpec);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Could not parse original importJobSpecs: %s", jobInfo.getRaw()), e);
    }
    if (existingImportJobSpec.getFeatureSpecsList().equals(importJobSpecs.getFeatureSpecsList())) {
      return;
    }

    Path workspace = PathUtil.getPath(defaults.getWorkspace()).resolve(importJobSpecs.getJobId());
    try {
      Files.createDirectory(workspace);
    } catch (FileAlreadyExistsException e) {
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Could not initialise job workspace job: %s", workspace.toString()), e);
    }

    writeImportJobSpecs(importJobSpecs, workspace);
    jobManager.updateJob(jobInfo, workspace);
    try {
      jobInfo.setRaw(JsonFormat.printer().print(importJobSpecs));
      jobInfoRepository.save(jobInfo);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          String.format("Could not serialize importJobSpecs: %s", importJobSpecs), e);
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
  public void updateJobStatus(String jobId, JobStatus status) {
    Optional<JobInfo> jobRecordOptional = jobInfoRepository.findById(jobId);
    if (jobRecordOptional.isPresent()) {
      JobInfo jobRecord = jobRecordOptional.get();
      jobRecord.setStatus(status);
      jobInfoRepository.save(jobRecord);
    }
  }

  public String getWorkspace() {
    return defaults.getWorkspace();
  }

  /**
   * Creates Import job specs from the given set of feast objects. This import job spec contains the
   * necessary information for a population import job to run.
   *
   * @param topic topic to listen to, will be used together with the store name as job name prefix
   * @param entitySpec entity spec of entity consumed by this job
   * @param featureSpecs feature specs of features consumed by this job
   * @param sinkStoreSpec storage spec of sink this job will write to
   * @param errorsStoreSpec storage spec of error sink this job will write to
   * @return ImportJobSpecs necessary to start a job
   */
  public ImportJobSpecs createImportJobSpecs(String topic, EntitySpec entitySpec,
      List<FeatureSpec> featureSpecs,
      StorageSpec sinkStoreSpec, StorageSpec errorsStoreSpec) {

    String jobNamePrefix = Strings.lenientFormat("%s-to-%s-", topic, sinkStoreSpec.getId())
        .toLowerCase();
    SourceSpec sourceSpec = SourceSpec.newBuilder()
        .setType(SourceType.valueOf(featureStream.getType().toUpperCase()))
        .putAllOptions(featureStream.getFeatureStreamOptions())
        .putOptions("topics", topic)
        .build();

    Builder importJobSpecsBuilder = ImportJobSpecs.newBuilder()
        .setJobId(createJobId(jobNamePrefix))
        .setSourceSpec(sourceSpec)
        .setEntitySpec(entitySpec)
        .addAllFeatureSpecs(featureSpecs)
        .setSinkStorageSpec(sinkStoreSpec)
        .setErrorsStorageSpec(errorsStoreSpec);

    return importJobSpecsBuilder.build();
  }

  private void writeImportJobSpecs(ImportJobSpecs importJobSpecs, Path workspace) {
    Path destination = workspace.resolve(IMPORT_JOB_SPECS_FILENAME);
    log.info("Writing ImportJobSpecs to {}", destination);
    try {
      String json = JsonFormat.printer().omittingInsignificantWhitespace().print(importJobSpecs);
      TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {
      };
      Map<String, Object> objectMap = new Gson().fromJson(json, typeToken.getType());
      ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
      String yaml = yamlMapper.writer().writeValueAsString(objectMap);
      Files.write(destination, Lists.newArrayList(yaml));
    } catch (JsonProcessingException | InvalidProtocolBufferException e) {
      throw new JobExecutionException("Cannot serialise to ImportJobSpecs to YAML", e);
    } catch (IOException e) {
      throw new JobExecutionException(
          String.format("Cannot write ImportJobSpecs to workspace %s", destination), e);
    }
  }

  private String createJobId(String namePrefix) {
    String dateSuffix = String.valueOf(Instant.now().toEpochMilli());
    return namePrefix.isEmpty() ? JOB_PREFIX_DEFAULT + dateSuffix : namePrefix + dateSuffix;
  }
}
