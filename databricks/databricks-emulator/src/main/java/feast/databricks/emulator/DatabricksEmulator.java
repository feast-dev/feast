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
package feast.databricks.emulator;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import feast.databricks.types.JobsCreateRequest;
import feast.databricks.types.JobsCreateResponse;
import feast.databricks.types.JobsDeleteRequest;
import feast.databricks.types.Library;
import feast.databricks.types.NewCluster;
import feast.databricks.types.ObjectMapperFactory;
import feast.databricks.types.Run;
import feast.databricks.types.RunLifeCycleState;
import feast.databricks.types.RunNowRequest;
import feast.databricks.types.RunNowResponse;
import feast.databricks.types.RunResultState;
import feast.databricks.types.RunState;
import feast.databricks.types.RunsCancelRequest;
import feast.databricks.types.RunsCancelResponse;
import feast.databricks.types.RunsGetResponse;
import feast.databricks.types.RunsListResponse;
import feast.databricks.types.RunsSubmitRequest;
import feast.databricks.types.RunsSubmitResponse;
import feast.databricks.types.SparkJarTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.ResponseTransformer;

public class DatabricksEmulator {

  static final String ENV_SPARK_HOME = "SPARK_HOME";

  private static final Logger log = LoggerFactory.getLogger(DatabricksEmulator.class);

  private static final ObjectMapper objectMapper = ObjectMapperFactory.createObjectMapper();

  public static void main(String[] args) {

    getSparkHome();

    setUpRestServices();
  }

  private static final String getSparkHome() {
    String path = System.getenv(ENV_SPARK_HOME);
    if (path == null) {
      throw new IllegalStateException(
          "Spark home not found; set it with the SPARK_HOME environment variable.");
    }
    return path;
  }

  private static void setUpRestServices() {

    EmulatorService emulator = new EmulatorService();

    JsonTransformer json = new JsonTransformer();

    port(8080);
    post("/api/2.0/jobs/create", emulator::jobsCreate, json);
    post("/api/2.0/jobs/delete", emulator::jobsDelete, json);
    post("/api/2.0/jobs/run-now", emulator::runNow, json);
    get("/api/2.0/jobs/runs/get", emulator::runsGet, json);
    post("/api/2.0/jobs/runs/cancel", emulator::runsCancel, json);
    post("/api/2.0/jobs/runs/submit", emulator::runsSubmit, json);
    get("/api/2.0/jobs/runs/list", emulator::runsList, json);
  }

  public static class JsonTransformer implements ResponseTransformer {

    @Override
    public String render(Object model) throws JsonProcessingException {
      return objectMapper.writeValueAsString(model);
    }
  }

  public static class ItemTracker<T> {
    private ConcurrentMap<Long, T> items = new ConcurrentHashMap<Long, T>();
    private AtomicLong lastId = new AtomicLong(0L);

    public T getItem(long id) {
      T item = items.get(id);
      if (item == null) {
        throw new IllegalArgumentException("ID does not exist: " + id);
      }
      return item;
    }

    public long addItem(T job) {
      long id = lastId.incrementAndGet();
      items.put(id, job);
      return id;
    }

    public void deleteItem(long id) {
      T item = items.remove(id);
      if (item == null) {
        throw new IllegalArgumentException("ID does not exist: " + id);
      }
    }
  }

  static class RunItem {
    final SparkAppHandle handle;
    final Optional<Long> jobId;
    final Optional<Long> numberInJob;

    RunItem(SparkAppHandle handle, Optional<Long> jobId, Optional<Long> numberInJob) {
      this.handle = handle;
      this.jobId = jobId;
      this.numberInJob = numberInJob;
    }
  }

  public static class EmulatorService {

    ItemTracker<JobsCreateRequest> jobTracker = new ItemTracker<>();

    ItemTracker<RunItem> runTracker = new ItemTracker<>();

    ConcurrentMap<Long, AtomicLong> runCountTracker = new ConcurrentHashMap<Long, AtomicLong>();

    SparkAppFactory appFactory = new SparkAppFactory();

    RunsGetResponse runsGet(Request request, Response response) throws Exception {
      long runId = Long.valueOf(request.queryParams("run_id"));
      log.info("Getting state for run {}", runId);

      RunState state = getRunState(runId);
      return RunsGetResponse.builder().setState(state).build();
    }

    private RunState getRunState(long runId) {
      RunItem item = runTracker.getItem(runId);
      SparkAppHandle handle = item.handle;

      RunState.Builder state = RunState.builder();

      switch (handle.getState()) {
        case CONNECTED:
          state.setLifeCycleState(RunLifeCycleState.PENDING);
          break;
        case FAILED:
          state.setLifeCycleState(RunLifeCycleState.TERMINATED);
          state.setResultState(RunResultState.FAILED);
          break;
        case FINISHED:
          state.setLifeCycleState(RunLifeCycleState.TERMINATED);
          state.setResultState(RunResultState.SUCCESS);
          break;
        case KILLED:
          state.setLifeCycleState(RunLifeCycleState.TERMINATED);
          state.setResultState(RunResultState.CANCELED);
          break;
        case LOST:
          state.setLifeCycleState(RunLifeCycleState.INTERNAL_ERROR);
          break;
        case RUNNING:
          state.setLifeCycleState(RunLifeCycleState.RUNNING);
          break;
        case SUBMITTED:
          state.setLifeCycleState(RunLifeCycleState.PENDING);
          break;
        case UNKNOWN:
          state.setLifeCycleState(RunLifeCycleState.PENDING);
          break;
        default:
          throw new IllegalStateException("Unexpected job state: " + handle.getState());
      }

      state.setStateMessage(
          String.format("Spark application %s in state %s", handle.getAppId(), handle.getState()));

      return state.build();
    }

    JobsCreateResponse jobsCreate(Request request, Response response) throws Exception {
      JobsCreateRequest req = objectMapper.readValue(request.body(), JobsCreateRequest.class);
      long jobId = jobTracker.addItem(req);
      log.info("Created job {}", jobId);
      return JobsCreateResponse.builder().setJobId(jobId).build();
    }

    JobsDeleteRequest jobsDelete(Request request, Response response) throws Exception {
      JobsDeleteRequest req = objectMapper.readValue(request.body(), JobsDeleteRequest.class);
      long jobId = req.getJobId();
      jobTracker.deleteItem(jobId);
      log.info("Deleted job {}", jobId);
      return req;
    }

    RunNowResponse runNow(Request request, Response response) throws Exception {
      RunNowRequest req = objectMapper.readValue(request.body(), RunNowRequest.class);
      long jobId = req.getJobId();
      log.info("Running job {}", jobId);

      JobsCreateRequest job = jobTracker.getItem(jobId);
      long numberInJob =
          runCountTracker.computeIfAbsent(jobId, k -> new AtomicLong(0L)).incrementAndGet();

      long runId =
          startRun(
              job.getLibraries(),
              job.getSparkJarTask(),
              job.getNewCluster(),
              Optional.of(jobId),
              Optional.of(numberInJob),
              req.getJarParams());

      log.info("Started job run {} for job {} (numberInJob: {})", runId, jobId, numberInJob);
      return RunNowResponse.builder().setRunId(runId).setNumberInJob(numberInJob).build();
    }

    private long startRun(
        Optional<List<Library>> libraries,
        SparkJarTask sparkJarTask,
        NewCluster newCluster,
        Optional<Long> jobId,
        Optional<Long> numberInJob,
        Optional<List<String>> runParams)
        throws IOException {

      List<String> jars = new ArrayList<>();
      libraries.ifPresent(
          libs -> {
            for (Library library : libs) {
              jars.add(library.getJar());
            }
          });

      List<String> params =
          runParams.orElse(sparkJarTask.getParameters().orElse(Collections.emptyList()));
      Map<String, String> sparkConf = newCluster.sparkConf().orElse(Collections.emptyMap());
      SparkAppHandle handle =
          appFactory.createApp(jars, sparkJarTask.getMainClassName(), params, sparkConf);

      RunItem item = new RunItem(handle, jobId, numberInJob);
      long runId = runTracker.addItem(item);
      return runId;
    }

    RunsListResponse runsList(Request request, Response response) throws Exception {
      log.info("Listing runs");

      String jobId = request.queryParams("job_id");
      String limit = request.queryParams("limit");
      Long jobIdValue = jobId == null ? null : Long.valueOf(jobId);
      // From API doc: The default value is 20. The max is 1000.
      // If a request specifies a limit of 0, the service will instead use the maximum limit.
      Long limitValue = limit == null ? 20 : Long.valueOf(limit);
      if (limitValue <= 0) {
        limitValue = 1000L;
      }
      if (limitValue > 1000L) {
        limitValue = 1000L;
      }
      List<Run> runs =
          runTracker.items.entrySet().stream()
              .filter(
                  e -> (jobIdValue == null) || jobIdValue.equals(e.getValue().jobId.orElse(null)))
              .sorted(Comparator.comparingLong(e -> -e.getValue().numberInJob.orElse(0L)))
              .limit(limitValue)
              .map(
                  e -> Run.builder().setRunId(e.getKey()).setState(getRunState(e.getKey())).build())
              .collect(Collectors.toList());

      return RunsListResponse.builder().setRuns(runs).build();
    }

    RunsCancelResponse runsCancel(Request request, Response response) throws Exception {
      RunsCancelRequest req = objectMapper.readValue(request.body(), RunsCancelRequest.class);

      long runId = req.getRunId();
      log.info("Canceling job run {}", runId);

      RunItem run = runTracker.getItem(runId);

      try {
        run.handle.stop();
      } catch (Exception e) {
        log.info("Application stopping threw exception", e);
      }

      return RunsCancelResponse.builder().build();
    }

    RunsSubmitResponse runsSubmit(Request request, Response response) throws Exception {
      RunsSubmitRequest req = objectMapper.readValue(request.body(), RunsSubmitRequest.class);

      long runId =
          startRun(
              req.getLibraries(),
              req.getSparkJarTask(),
              req.getNewCluster(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      log.info("Started job run {}", runId);
      return RunsSubmitResponse.builder().setRunId(runId).build();
    }
  }

  public static class SparkAppFactory {

    SparkAppHandle createApp(
        List<String> jars,
        String mainClassName,
        List<String> parameters,
        Map<String, String> sparkConf)
        throws IOException {
      SparkLauncher launcher = new SparkLauncher();
      String appResource = null;
      for (String jar : jars) {
        launcher.addJar(jar);
        appResource = jar;
      }
      SparkLauncher builder =
          launcher
              .setMainClass(mainClassName)
              .setMaster("local")
              .setAppResource(appResource)
              .addAppArgs((String[]) parameters.toArray(new String[parameters.size()]))
              .setConf(SparkLauncher.DRIVER_MEMORY, "1g");
      sparkConf.forEach((k, v) -> builder.setConf(k, v));
      return builder.startApplication();
    }
  }
}
