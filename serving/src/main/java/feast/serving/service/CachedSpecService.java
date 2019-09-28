package feast.serving.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.GetStoresRequest;
import feast.core.CoreServiceProto.GetStoresRequest.Filter;
import feast.core.CoreServiceProto.GetStoresResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.exception.SpecRetrievalException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * SpecStorage implementation with built-in in-memory cache.
 */
public class CachedSpecService implements SpecService {

  private static final int MAX_SPEC_COUNT = 1000;

  private final SpecService coreService;
  private final String storeId;

  private final CacheLoader<String, FeatureSetSpec> featureSetSpecCacheLoader;
  private final LoadingCache<String, FeatureSetSpec> featureSetSpecCache;
  private Store store;

  public CachedSpecService(SpecService coreService, String storeId) {
    this.storeId = storeId;
    this.coreService = coreService;

    GetStoresResponse stores = coreService.getStores(
        GetStoresRequest.newBuilder()
            .setFilter(Filter.newBuilder()
                .setName(storeId))
            .build());
    this.store = stores.getStore(0);

    Map<String, FeatureSetSpec> featureSetSpecs = getFeatureSetSpecMap();
    featureSetSpecCacheLoader =
        CacheLoader.from(
            (String key) -> featureSetSpecs.get(key));
    featureSetSpecCache =
        CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(featureSetSpecCacheLoader);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GetStoresResponse getStores(GetStoresRequest getStoresRequest) {
    return GetStoresResponse.newBuilder().addStore(this.store).build();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GetFeatureSetsResponse getFeatureSets(GetFeatureSetsRequest getFeatureSetsRequest) {
    if (getFeatureSetsRequest.getFilter().getFeatureSetName().equals("")) {
      return GetFeatureSetsResponse.newBuilder()
          .addAllFeatureSets(featureSetSpecCache.asMap().values())
          .build();
    }

    GetFeatureSetsRequest.Filter filter = getFeatureSetsRequest.getFilter();
    String id = String.format("%s:%s", filter.getFeatureSetName(), filter.getFeatureSetVersion());
    try {
      return GetFeatureSetsResponse.newBuilder()
          .addFeatureSets(featureSetSpecCache.get(id))
          .build();
    } catch (InvalidCacheLoadException e) {
      // if not found, default to core
      return coreService.getFeatureSets(getFeatureSetsRequest);
    } catch (ExecutionException e) {
      throw new SpecRetrievalException(
          String.format("Unable to retrieve featureSet with id %s", id), e);
    }
  }

  /**
   * Preload all specs into the cache.
   */
  public void populateCache() {
    GetStoresResponse stores = coreService.getStores(
        GetStoresRequest.newBuilder()
            .setFilter(Filter.newBuilder()
                .setName(storeId))
            .build());
    this.store = stores.getStore(0);

    Map<String, FeatureSetSpec> featureSetSpecMap = getFeatureSetSpecMap();
    featureSetSpecCache.putAll(featureSetSpecMap);
  }

  private Map<String, FeatureSetSpec> getFeatureSetSpecMap() {
    HashMap<String, FeatureSetSpec> featureSetSpecs = new HashMap<>();

    for (Subscription subscription : this.store.getSubscriptionsList()) {
      GetFeatureSetsResponse featureSetsResponse = coreService
          .getFeatureSets(GetFeatureSetsRequest.newBuilder()
              .setFilter(
                  GetFeatureSetsRequest.Filter.newBuilder()
                      .setFeatureSetName(subscription.getName())
                      .setFeatureSetVersion(subscription.getVersion())
              ).build());

      for (FeatureSetSpec featureSetSpec : featureSetsResponse.getFeatureSetsList()) {
        featureSetSpecs
            .put(String.format("%s:%s", featureSetSpec.getName(), featureSetSpec.getVersion()),
                featureSetSpec);
      }
    }
    return featureSetSpecs;
  }
}
