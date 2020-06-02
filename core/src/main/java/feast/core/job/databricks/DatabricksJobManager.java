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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
            String token,
            HttpClient httpClient) {

        DatabricksJobConfig config = new DatabricksJobConfig(runnerConfigOptions.get("databricksHost"));
        this.databricksHost = config.getDatabricksHost();
        this.defaultOptions = runnerConfigOptions;
        this.metricsProperties = metricsProperties;
        this.httpClient = httpClient;
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


    @SneakyThrows
    @Override
    public JobStatus getJobStatus(Job job) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(String.format("%s/api/2.0/jobs/get?job_id=%s", this.databricksHost, job.getExtId())))
                .header("Authorization", String.format("%s %s", "Bearer", this.databricksToken))
                .build();
        HttpResponse<String> response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        //todo add mapping logic
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

        if (response.statusCode() == 200) {
            String runId = parent.path("run_id").asText();
            return new Job(jobId, runId, getRunnerType().name(), Source.fromProto(source), Store.fromProto(sink), featureSets, JobStatus.PENDING);
        } else {
            throw new RuntimeException(String.format("Failed running of job %s: %s", jobId, response.body())); // TODO: handle failure
        }
    }

//    private ArrayNode getJarParams(SourceProto.Source source, StoreProto.Store sink, List<FeatureSetProto.FeatureSet> featureSets) {
//
//
//    }

}
