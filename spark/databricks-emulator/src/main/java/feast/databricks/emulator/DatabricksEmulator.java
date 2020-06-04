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
import feast.databricks.types.Library;
import feast.databricks.types.RunLifeCycleState;
import feast.databricks.types.RunResultState;
import feast.databricks.types.RunState;
import feast.databricks.types.RunsGetResponse;
import feast.databricks.types.RunsSubmitRequest;
import feast.databricks.types.RunsSubmitResponse;
import feast.databricks.types.SparkJarTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import spark.Request;
import spark.Response;
import spark.ResponseTransformer;

public class DatabricksEmulator {

  ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) {
    port(8080);

    EmulatorService emulator = new EmulatorService();

    JsonTransformer json = new JsonTransformer();

    post("/2.0/jobs/runs/submit", emulator::runsSubmit, json);

    get("/2.0/jobs/runs/get", emulator::runsGet, json);
  }

  public static class JsonTransformer implements ResponseTransformer {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String render(Object model) throws JsonProcessingException {
      return objectMapper.writeValueAsString(model);
    }
  }

  public static class RunTracker {
    private ConcurrentMap<Long, SparkAppHandle> runs =
        new ConcurrentHashMap<Long, SparkAppHandle>();
    private AtomicLong lastRunId = new AtomicLong(0L);

    public SparkAppHandle getRun(long runId) {
      SparkAppHandle h = runs.get(runId);
      if (h == null) {
        throw new IllegalArgumentException("Run ID: " + runId);
      }
      return h;
    }

    public long addRun(SparkAppHandle handle) {
      long runId = lastRunId.incrementAndGet();
      runs.put(runId, handle);
      return runId;
    }
  }

  public static class EmulatorService {

    RunTracker runTracker = new RunTracker();

    SparkAppFactory appFactory = new SparkAppFactory();

    ObjectMapper objectMapper = new ObjectMapper();

    RunsGetResponse runsGet(Request request, Response response) throws Exception {
      long runId = Long.valueOf(request.queryParams("run_id"));

      RunState state = getRunState(runId);
      return RunsGetResponse.builder().setState(state).build();
    }

    private RunState getRunState(long runId) {
      SparkAppHandle handle = runTracker.getRun(runId);

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

      state.setStateMessage(handle.getState().toString());

      return state.build();
    }

    RunsSubmitResponse runsSubmit(Request request, Response response) throws Exception {
      RunsSubmitRequest req = objectMapper.readValue(request.body(), RunsSubmitRequest.class);

      List<String> jars = new ArrayList<>();
      for (Library library : req.getLibraries()) {
        jars.add(library.getJar());
      }

      SparkJarTask task = req.getSparkJarTask();
      SparkAppHandle handle =
          appFactory.createApp(jars, task.getMainClassName(), task.getParameters());

      long run_id = runTracker.addRun(handle);
      return RunsSubmitResponse.builder().setRunId(run_id).build();
    }
  }

  public static class SparkAppFactory {

    SparkAppHandle createApp(List<String> jars, String mainClassName, List<String> parameters)
        throws IOException {
      SparkLauncher launcher = new SparkLauncher();
      String appResource = null;
      for (String jar : jars) {
        launcher.addJar(jar);
        appResource = jar;
      }
      return launcher //
          .setMainClass(mainClassName) //
          .setMaster("local") //
          .setAppResource(appResource) //
          .setConf(SparkLauncher.DRIVER_MEMORY, "1g") //
          .addAppArgs((String[]) parameters.toArray(new String[parameters.size()]))
          .startApplication();
    }
  }
}
