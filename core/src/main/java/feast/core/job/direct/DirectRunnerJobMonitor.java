package feast.core.job.direct;

import com.google.common.base.Strings;
import feast.core.job.JobMonitor;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import feast.core.model.Metrics;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.DefaultExecuteResultHandler;

@Slf4j
public class DirectRunnerJobMonitor implements JobMonitor {

  private final DirectJobRegistry jobs;

  public DirectRunnerJobMonitor(DirectJobRegistry jobs) {
    this.jobs = jobs;
  }

  @Override
  public JobStatus getJobStatus(JobInfo job) {
    String jobId = job.getExtId();
    DirectJob directJob = jobs.get(jobId);
    DefaultExecuteResultHandler resultHandler = directJob
        .getResultHandler();
    if (resultHandler.hasResult()) {
      int exitCode = resultHandler.getExitValue();
      if (exitCode == 0) {
        return JobStatus.COMPLETED;
      } else {
        log.error(
            Strings.lenientFormat("Direct runner job with id %s failed: %s", jobId,
                resultHandler.getException().toString()));
        return JobStatus.ERROR;
      }
    }
    if (directJob.getWatchdog().isWatching()) {
      return JobStatus.RUNNING;
    }
    return JobStatus.UNKNOWN;
  }

  @Override
  public List<Metrics> getJobMetrics(JobInfo job) {
    // Direct runner provides no job metrics
    return new ArrayList<>();
  }
}
