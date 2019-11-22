package feast.core.job.direct;

import feast.core.model.JobStatus;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult.State;

public class DirectJobStateMapper {

  private static final Map<State, JobStatus> BEAM_TO_FEAT_JOB_STATUS;

  static {
    BEAM_TO_FEAT_JOB_STATUS = new HashMap<>();
    BEAM_TO_FEAT_JOB_STATUS.put(State.FAILED, JobStatus.ERROR);
    BEAM_TO_FEAT_JOB_STATUS.put(State.RUNNING, JobStatus.RUNNING);
    BEAM_TO_FEAT_JOB_STATUS.put(State.UNKNOWN, JobStatus.UNKNOWN);
    BEAM_TO_FEAT_JOB_STATUS.put(State.CANCELLED, JobStatus.ABORTED);
    BEAM_TO_FEAT_JOB_STATUS.put(State.DONE, JobStatus.COMPLETED);
    BEAM_TO_FEAT_JOB_STATUS.put(State.STOPPED, JobStatus.ABORTED);
    BEAM_TO_FEAT_JOB_STATUS.put(State.UPDATED, JobStatus.RUNNING);
  }

  /**
   * Map a dataflow job state to Feast's JobStatus
   *
   * @param jobState beam PipelineResult State
   * @return JobStatus
   */
  public JobStatus map(State jobState) {
    return BEAM_TO_FEAT_JOB_STATUS.get(jobState);
  }
}
