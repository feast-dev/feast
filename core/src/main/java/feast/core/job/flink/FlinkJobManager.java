package feast.core.job.flink;

import feast.core.config.ImportJobDefaults;
import feast.core.job.JobManager;
import feast.core.util.TypeConversion;
import feast.specs.ImportSpecProto.ImportSpec;
import java.util.ArrayList;
import java.util.Base64;
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
  public String submitJob(ImportSpec importSpec, String jobId) {
    flinkCli.parseParameters(createRunArgs(importSpec, jobId));

    return getFlinkJobId(jobId);
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

  private String[] createRunArgs(ImportSpec importSpec, String jobId) {
    Map<String, String> options =
        TypeConversion.convertJsonStringToMap(defaults.getImportJobOptions());
    List<String> commands = new ArrayList<>();
    commands.add("run");
    commands.add("-d");
    commands.add("-m");
    commands.add(masterUrl);
    commands.add(defaults.getExecutable());
    commands.add(option("jobName", jobId));
    commands.add(option("runner", defaults.getRunner()));
    commands.add(
        option("importSpecBase64", Base64.getEncoder().encodeToString(importSpec.toByteArray())));
    commands.add(option("coreApiUri", defaults.getCoreApiUri()));
    commands.add(option("errorsStoreType", defaults.getErrorsStoreType()));
    commands.add(option("errorsStoreOptions", defaults.getErrorsStoreOptions()));

    options.forEach((k, v) -> commands.add(option(k, v)));
    return commands.toArray(new String[] {});
  }

  private String[] createStopArgs(String extId) {
    List<String> commands = new ArrayList<>();
    commands.add("cancel");
    commands.add("-m");
    commands.add(masterUrl);
    commands.add(extId);
    return commands.toArray(new String[] {});
  }

  private String option(String key, String value) {
    return String.format("--%s=%s", key, value);
  }
}
