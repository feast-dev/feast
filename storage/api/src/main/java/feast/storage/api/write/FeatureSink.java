package feast.storage.api.write;

import feast.types.FeatureRowProto.FeatureRow;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

/**
 * Interface for implementing user defined feature sink functionality.
 */
public interface FeatureSink extends Serializable {

    /**
     *
     * @param input
     * @return
     */
    WriteResult write(PCollection<FeatureRow> input);
}
