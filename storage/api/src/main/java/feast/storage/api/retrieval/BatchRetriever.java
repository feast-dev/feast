package feast.storage.api.retrieval;

import feast.serving.ServingAPIProto;

import java.io.Serializable;

/**
 * Interface for implementing user defined retrieval functionality from
 * Batch/historical stores.
 */
public interface BatchRetriever extends Serializable {

    /**
     * Get all features corresponding to the provided batch features request.
     *
     * @param request {@link ServingAPIProto.GetBatchFeaturesRequest} containing requested features and file containing entity columns.
     * @return {@link ServingAPIProto.Job} if successful, contains the location of the results, else contains the error to be returned to the user.
     */
    ServingAPIProto.Job getFeatures(ServingAPIProto.GetBatchFeaturesRequest request);
}
