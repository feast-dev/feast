package feast.core.job.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import feast.core.model.JobInfo;
import feast.core.model.JobStatus;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.junit.Test;
import org.mockito.Mockito;

public class DirectRunnerJobMonitorTest {

  @Test
  public void shouldReturnJobCompletedIfJobCompleted() {
    DefaultExecuteResultHandler resultHandler = Mockito.mock(DefaultExecuteResultHandler.class);
    when(resultHandler.hasResult()).thenReturn(true);
    when(resultHandler.getExitValue()).thenReturn(0);
    DirectJob directJob = new DirectJob("myJob", new ExecuteWatchdog(1), resultHandler);
    DirectJobRegistry jobs = new DirectJobRegistry();
    jobs.add(directJob);
    DirectRunnerJobMonitor jobMonitor = new DirectRunnerJobMonitor(jobs);

    JobInfo jobInfo = new JobInfo();
    jobInfo.setExtId("myJob");

    JobStatus actual = jobMonitor.getJobStatus(jobInfo);
    assertThat(actual, equalTo(JobStatus.COMPLETED));
  }

  @Test
  public void shouldReturnJobErrorIfJobError() {
    DefaultExecuteResultHandler resultHandler = Mockito.mock(DefaultExecuteResultHandler.class);
    when(resultHandler.hasResult()).thenReturn(true);
    when(resultHandler.getExitValue()).thenReturn(1);
    when(resultHandler.getException()).thenReturn(new ExecuteException("failed", 1));

    DirectJob directJob = new DirectJob("myJob", new ExecuteWatchdog(1), resultHandler);
    DirectJobRegistry jobs = new DirectJobRegistry();
    jobs.add(directJob);
    DirectRunnerJobMonitor jobMonitor = new DirectRunnerJobMonitor(jobs);
    JobInfo jobInfo = new JobInfo();
    jobInfo.setExtId("myJob");

    JobStatus actual = jobMonitor.getJobStatus(jobInfo);
    assertThat(actual, equalTo(JobStatus.ERROR));
  }

  @Test
  public void shouldReturnJobRunningIfJobHasNotTerminated() {
    DefaultExecuteResultHandler resultHandler = Mockito.mock(DefaultExecuteResultHandler.class);
    when(resultHandler.hasResult()).thenReturn(false);
    ExecuteWatchdog watchdog = Mockito.mock(ExecuteWatchdog.class);
    when(watchdog.isWatching()).thenReturn(true);

    DirectJob directJob = new DirectJob("myJob", watchdog, resultHandler);
    DirectJobRegistry jobs = new DirectJobRegistry();
    jobs.add(directJob);
    DirectRunnerJobMonitor jobMonitor = new DirectRunnerJobMonitor(jobs);
    JobInfo jobInfo = new JobInfo();
    jobInfo.setExtId("myJob");

    JobStatus actual = jobMonitor.getJobStatus(jobInfo);
    assertThat(actual, equalTo(JobStatus.RUNNING));
  }
}