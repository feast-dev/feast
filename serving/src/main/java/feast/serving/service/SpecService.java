package feast.serving.service;

import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresResponse;

public interface SpecService {
  GetStoresResponse getStores(GetStoresRequest getStoresRequest);

  GetFeatureSetsResponse getFeatureSets(GetFeatureSetsRequest getFeatureSetsRequest);
}
