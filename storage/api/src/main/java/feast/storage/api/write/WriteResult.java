package feast.storage.api.write;

import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

/**
 * The result of a write transform.
 */
public interface WriteResult extends Serializable {

    /**
     * Gets set of successfully written feature rows.
     *
     * @return PCollection of feature rows successfully written to the store
     */
    PCollection<FeatureRow> getSuccessfulInserts();

    /**
     * Gets set of feature rows that were unsuccessfully written to the store.
     * The failed feature rows are wrapped in FailedElement objects so implementations
     * of WriteResult can be flexible in how errors are stored.
     *
     * @return FailedElements of unsuccessfully written feature rows
     */
    PCollection<FailedElement> getFailedInserts();
}
