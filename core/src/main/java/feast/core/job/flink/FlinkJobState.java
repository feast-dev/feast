package feast.core.job.flink;

/**
 * Possible state of flink's job.
 */
public enum FlinkJobState {

  /** Job is newly created */
  CREATED,

  /** Job is running */
  RUNNING,

  /** job is completed successfully */
  FINISHED,

  /** job is reset and restarting */
  RESTARTING,

  /** job is being canceled */
  CANCELLING,

  /** job has ben cancelled */
  CANCELED,

  /** job has failed and waiting for cleanup */
  FAILING,

  /** job has failed with a non-recoverable failure */
  FAILED,

  /** job has been suspended and waiting for cleanup */
  SUSPENDING,

  /** job has been suspended */
  SUSPENDED,

  /** job is reconciling and waits for task execution to recover state */
  RECONCILING
}
