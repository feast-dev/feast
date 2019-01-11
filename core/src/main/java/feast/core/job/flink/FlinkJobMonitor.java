package feast.core.job.flink;

import feast.core.job.JobMonitor;
import feast.core.job.Runner;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkJobMonitor implements JobMonitor {

  private final FlinkRestApi flinkRestApi;
  private final FlinkJobMapper mapper;

  public FlinkJobMonitor(FlinkRestApi flinkRestApi) {
    this.flinkRestApi = flinkRestApi;
    this.mapper = new FlinkJobMapper();
  }

  @Override
  public JobStatus getJobStatus(JobInfo jobInfo) {
    if (!Runner.FLINK.getName().equals(jobInfo.getRunner())) {
      return jobInfo.getStatus();
    }

    FlinkJobList jobList = flinkRestApi.getJobsOverview();
    for (FlinkJob job : jobList.getJobs()) {
      if (jobInfo.getExtId().equals(job.getJid())) {
        return mapFlinkJobStatusToFeastJobStatus(job.getState());
      }
    }
    return JobStatus.UNKNOWN;
  }

  @Override
  public List<Metrics> getJobMetrics(JobInfo job) {
    if (!Runner.FLINK.getName().equals(job.getRunner())) {
      return null;
    }
    // TODO: metrics for flink
    return Collections.emptyList();
  }

  private JobStatus mapFlinkJobStatusToFeastJobStatus(String state) {
    try {
      return mapper.map(state);
    } catch (IllegalArgumentException e) {
      log.error("Unknown job state: " + state);
      return JobStatus.UNKNOWN;
    }
  }
}
