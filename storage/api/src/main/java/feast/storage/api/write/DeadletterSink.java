package feast.storage.api.write;

import org.apache.beam.sdk.values.PCollection;

/**
 * Interface for for implementing user defined deadletter sinks
 * to write failed elements to.
 */
public interface DeadletterSink {

    /**
     * Write collection of FailedElements to the deadletter sink.
     *
     * @param input PCollection of FailedElements
     */
    void write(PCollection<FailedElement> input);
}
