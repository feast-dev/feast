package feast.core.job.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.FeatureSetProto;
import feast.core.SourceProto;
import feast.core.StoreProto;
import feast.core.config.FeastProperties.MetricsProperties;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DatabricksJobManager implements JobManager {
    private static Gson gson = new Gson();

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

        DatabricksJobConfig config = new DatabricksJobConfig(runnerConfigOptions.get("databricksHost"));
        this.databricksHost = config.getDatabricksHost();
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


            return runDatabricksJob(
                    job.getId(),
                    featureSetProtos,
                    job.getSource().toProto(),
                    job.getStore().toProto());
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
     * Update an existing Databricks job.
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


    @Override
    public JobStatus getJobStatus(Job job) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s/api/2.0/jobs/runs/get?run_id=%s", this.databricksHost, job.getExtId())))
                .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
                .build();
        try {
            HttpResponse<String> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode parent = new ObjectMapper().readTree(response.body());
            Optional<String> resultState = Optional.of(parent.path("state").get("result_state").asText().toUpperCase());
            String lifeCycleState = parent.path("state").path("life_cycle_state").asText().toUpperCase();

            if (resultState.isPresent()) {
                return DatabricksJobStateMapper.map(String.format("%s_%s", lifeCycleState, resultState.get()));
            }

            return DatabricksJobStateMapper.map(lifeCycleState);
        } catch (IOException | InterruptedException ex) {
            log.error(
                    "Unable to retrieve status of a dabatricks run with id : {}\ncause: {}",
                    job.getExtId(),
                    ex.getMessage());
        }

        return JobStatus.UNKNOWN;
    }

    @SneakyThrows
    private Job runDatabricksJob(
            String jobId,
            List<FeatureSetProto.FeatureSet> featureSetProtos,
            SourceProto.Source source,
            StoreProto.Store sink) {

        List<FeatureSet> featureSets =
                featureSetProtos.stream().map(FeatureSet::fromProto).collect(Collectors.toList());

        ObjectMapper mapper = new ObjectMapper();
        ArrayNode jarParams = mapper.createArrayNode();
        ObjectNode body = mapper.createObjectNode();
        body.put("job_id", jobId);
        body.set("jar_params", jarParams);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s/api/2.0/jobs/run-now", this.databricksHost)))
                .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        JsonNode parent = new ObjectMapper().readTree(response.body());
        String runId = parent.path("run_id").asText();

        Job job = new Job(jobId, runId, getRunnerType().name(), Source.fromProto(source), Store.fromProto(sink), featureSets, JobStatus.PENDING);
        job.setExtId(runId);

        return job;
    }

}
