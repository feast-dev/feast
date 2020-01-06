package feast.retry;

public interface Retriable {
    void execute();
    Boolean isExceptionRetriable(Exception e);
    void cleanUpAfterFailure();
}
