package feast.core.job.direct;

import feast.core.job.JobMonitor;
import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DirectRunnerJobMonitor implements JobMonitor {

  private final DirectJobRegistry jobs;
  private final DirectJobStateMapper jobStateMapper;

  public DirectRunnerJobMonitor(DirectJobRegistry jobs) {
    this.jobs = jobs;
    jobStateMapper = new DirectJobStateMapper();
  }

  @Override
  public JobStatus getJobStatus(JobInfo job) {
    DirectJob directJob = jobs.get(job.getId());
    if (directJob == null) {
      return JobStatus.ABORTED;
    }
    return jobStateMapper.map(directJob.getPipelineResult().getState());
  }
}
