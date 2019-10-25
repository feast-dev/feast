package feast.retry;

import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.Serializable;

public class BackOffExecutor implements Serializable {

    private static FluentBackoff backoff;

    public BackOffExecutor(Integer maxRetries, Duration initialBackOff) {
        backoff = FluentBackoff.DEFAULT
                .withMaxRetries(maxRetries)
                .withInitialBackoff(initialBackOff);
    }

    public void execute(Retriable retriable) throws Exception {
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backOff = backoff.backoff();
        while(true) {
            try {
                retriable.execute();
                break;
            } catch (Exception e) {
                if(retriable.isExceptionRetriable(e) && BackOffUtils.next(sleeper, backOff)) {
                    retriable.cleanUpAfterFailure();
                } else {
                    throw e;
                }
            }
        }
    }
}
