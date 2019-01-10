package feast.core.job.flink;

import feast.core.model.JobStatus;
import java.util.HashMap;
import java.util.Map;

public class FlinkJobMapper {
  private static final Map<FlinkJobState, JobStatus> FLINK_TO_FEAST_JOB_STATE_MAP;

  static {
    FLINK_TO_FEAST_JOB_STATE_MAP = new HashMap<>();
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.CREATED, JobStatus.PENDING);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.RUNNING, JobStatus.RUNNING);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.FINISHED, JobStatus.COMPLETED);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.RESTARTING, JobStatus.RUNNING);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.CANCELLING, JobStatus.ABORTING);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.CANCELED, JobStatus.ABORTED);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.FAILING, JobStatus.ERROR);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.FAILED, JobStatus.ERROR);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.SUSPENDING, JobStatus.SUSPENDING);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.SUSPENDED, JobStatus.SUSPENDED);
    FLINK_TO_FEAST_JOB_STATE_MAP.put(FlinkJobState.RECONCILING, JobStatus.PENDING);
  }

  /**
   * Map a string containing Flink's JobState into Feast's JobStatus
   *
   * @param jobState Flink JobState
   * @return JobStatus.
   * @throws IllegalArgumentException if jobState is invalid.
   */
  public JobStatus map(String jobState) {
    FlinkJobState dfJobState = FlinkJobState.valueOf(jobState);
    if (FLINK_TO_FEAST_JOB_STATE_MAP.containsKey(dfJobState)) {
      return FLINK_TO_FEAST_JOB_STATE_MAP.get(dfJobState);
    }
    throw new IllegalArgumentException("Unknown job state: " + jobState);
  }
}
