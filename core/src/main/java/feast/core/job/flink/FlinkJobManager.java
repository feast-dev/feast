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

package feast.core.job.flink;

import feast.core.config.ImportJobDefaults;
import feast.core.job.JobManager;
import feast.core.util.TypeConversion;
import feast.specs.ImportJobSpecsProto.ImportJobSpecs;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.client.cli.CliFrontend;

@Slf4j
public class FlinkJobManager implements JobManager {

  private final CliFrontend flinkCli;
  private final ImportJobDefaults defaults;
  private final String masterUrl;
  private final FlinkRestApi flinkRestApis;

  public FlinkJobManager(
      CliFrontend flinkCli,
      FlinkJobConfig config,
      FlinkRestApi flinkRestApi,
      ImportJobDefaults defaults) {
    this.flinkCli = flinkCli;
    this.defaults = defaults;
    this.masterUrl = config.getMasterUrl();
    this.flinkRestApis = flinkRestApi;
  }

  @Override
  public String submitJob(ImportJobSpecs importJobSpecs, Path workspace) {
    flinkCli.parseParameters(createRunArgs(importJobSpecs, workspace));

    return getFlinkJobId(importJobSpecs.getJobId());
  }

  @Override
  public void abortJob(String extId) {
    flinkCli.parseParameters(createStopArgs(extId));
  }

  private String getFlinkJobId(String jobId) {
    FlinkJobList jobList = flinkRestApis.getJobsOverview();
    for (FlinkJob job : jobList.getJobs()) {
      if (jobId.equals(job.getName())) {
        return job.getJid();
      }
    }
    log.warn("Unable to find job: {}", jobId);
    return "";
  }

  private String[] createRunArgs(ImportJobSpecs importJobSpecs, Path workspace) {
    Map<String, String> options =
        TypeConversion.convertJsonStringToMap(defaults.getImportJobOptions());
    List<String> commands = new ArrayList<>();
    commands.add("run");
    commands.add("-d");
    commands.add("-m");
    commands.add(masterUrl);
    commands.add(defaults.getExecutable());
    commands.add(option("jobName", importJobSpecs.getJobId()));
    commands.add(option("runner", defaults.getRunner()));
    commands.add(option("workspace", workspace.toString()));

    options.forEach((k, v) -> commands.add(option(k, v)));
    return commands.toArray(new String[]{});
  }

  private String[] createStopArgs(String extId) {
    List<String> commands = new ArrayList<>();
    commands.add("cancel");
    commands.add("-m");
    commands.add(masterUrl);
    commands.add(extId);
    return commands.toArray(new String[]{});
  }

  private String option(String key, String value) {
    return String.format("--%s=%s", key, value);
  }
}
