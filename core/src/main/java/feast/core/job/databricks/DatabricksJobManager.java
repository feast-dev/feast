package feast.core.job.databricks;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import feast.core.model.JobStatus;
import feast.core.FeatureSetProto;
import feast.core.SourceProto;
import feast.core.StoreProto;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class DatabricksJobManager implements JobManager {

    private final Runner RUNNER_TYPE = Runner.DATABRICKS;

    private final String databricksHost;
    private final String databricksToken;
    private final Map<String, String> defaultOptions;
    private final MetricsProperties metricsProperties;
    private final HttpClient httpClient;

    public DatabricksJobManager(
            Map<String, String> runnerConfigOptions,
            MetricsProperties metricsProperties,
            String token) {

        DatabricksRunnerConfig config = new DatabricksRunnerConfig(runnerConfigOptions);
        this.databricksHost = config.databricksService;
        this.defaultOptions = runnerConfigOptions;
        this.metricsProperties = metricsProperties;
        this.httpClient = HttpClient.newHttpClient();
        this.databricksToken = token;

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
    public Job restartJob(Job job) {
        return null;
    }


    @SneakyThrows
    @Override
    public JobStatus getJobStatus(Job job) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s/api/2.0/jobs/get?job_id=%s", this.databricksHost, job.getExtId())))
                .header("Authorization", String.format("%s %s","Bearer" ,this.databricksToken))
                .build();
        HttpResponse<String> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        //todo add mapping logic
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
