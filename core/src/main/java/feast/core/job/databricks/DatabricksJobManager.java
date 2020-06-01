package feast.core.job.databricks;

import com.google.api.client.auth.oauth2.Credential; // Needs to be swapped for databricks credential.
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.exception.JobExecutionException;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.SourceProto;
import feast.proto.core.StoreProto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatabricksJobManager implements JobManager {

    private final Runner RUNNER_TYPE = Runner.DATABRICKS;

    private final String databricksService;
    private final Map<String, String> defaultOptions;
    private final MetricsProperties metricsProperties;

    public DatabricksJobManager(
            Map<String, String> runnerConfigOptions,
            MetricsProperties metricsProperties,
            Credential credential) {

        DatabricksRunnerConfig config = new DatabricksRunnerConfig(runnerConfigOptions);
        this.databricksService = config.databricksService;
        this.defaultOptions = runnerConfigOptions;
        this.metricsProperties = metricsProperties;

    }

    @Override
    public Runner getRunnerType() {
        return RUNNER_TYPE;
    }

    @Override
    public Job startJob(Job job) {
        try {
            List<FeatureSetProto.FeatureSet> featureSetProtos = new ArrayList<>();
            for (FeatureSet featureSet : job.getFeatureSets()) {
                featureSetProtos.add(featureSet.toProto());
            }
            String extId =
                    submitDatabricksJob(
                            job.getId(),
                            featureSetProtos,
                            job.getSource().toProto(),
                            job.getStore().toProto(),
                            false);
            job.setExtId(extId);
            return job;

        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage());
            throw new IllegalArgumentException(
                    String.format(
                            "DatabricksJobManager failed to START job with id '%s' because the job"
                                    + "has an invalid spec. Please check the FeatureSet, Source and Store specs. Actual error message: %s",
                            job.getId(), e.getMessage()));
        }

    }

    /**
     * Update an existing Dataflow job.
     *
     * @param job job of target job to change
     * @return Databricks-specific job id
     */
    @Override
    public Job updateJob(Job job) {
        return job;
    }

    @Override
    public void abortJob(String jobId) {
    }


    @Override
    public Job restartJob(Job job){
        if (job.getStatus().isTerminal()) {
            // job yet not running: just start job
            return this.startJob(job);
        } else {
            // job is running - updating the job without changing the job has
            // the effect of restarting the job
            return this.updateJob(job);
        }
    }

    @Override
    public JobStatus getJobStatus(Job job) {
        return JobStatus.UNKNOWN;
    }

    private String submitDatabricksJob(
            String jobName,
            List<FeatureSetProto.FeatureSet> featureSetProtos,
            SourceProto.Source source,
            StoreProto.Store sink,
            boolean update) {
        return "EXT_ID";
    }




    }
