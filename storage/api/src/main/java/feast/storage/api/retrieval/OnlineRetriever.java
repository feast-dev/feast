package feast.storage.api.retrieval;

import feast.serving.ServingAPIProto.GetOnlineFeaturesRequest;
import feast.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.types.FeatureRowProto;

import java.io.Serializable;
import java.util.List;

/**
 * Interface for implementing user defined retrieval functionality from
 * Online stores.
 */
public interface OnlineRetriever<T> extends Serializable {

    /**
     * Get all values corresponding to the request.
     *
     * @param request Online features request containing the list of features and list of entity rows
     * @return list of {@link T}
     */
    List<T> getFeatures(GetOnlineFeaturesRequest request);

    /**
     * Checks whether the response is empty, i.e. feature does not exist in the store
     *
     * @param response {@link T}
     * @return boolean
     */
    boolean isEmpty(T response);

    /**
     * Parse response from data store to FeatureRow
     *
     * @param response {@link T}
     * @return {@link FeatureRowProto.FeatureRow}
     */
    FeatureRowProto.FeatureRow parseResponse(T response);
}
