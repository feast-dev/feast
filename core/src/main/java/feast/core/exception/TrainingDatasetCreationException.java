package feast.core.exception;

/**
 * Exception that happens when creation of training dataset failed.
 */
public class TrainingDatasetCreationException extends RuntimeException {
  public TrainingDatasetCreationException() {
    super();
  }

  public TrainingDatasetCreationException(String message) {
    super(message);
  }

  public TrainingDatasetCreationException(String message, Throwable cause) {
    super(message, cause);
  }
}
