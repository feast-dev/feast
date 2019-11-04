package feast.serving.service;

import static feast.serving.util.mappers.YamlToProtoMapper.yamlToStoreProto;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import feast.core.CoreServiceProto.GetFeatureSetsRequest;
import feast.core.CoreServiceProto.GetFeatureSetsRequest.Filter;
import feast.core.CoreServiceProto.GetFeatureSetsResponse;
import feast.core.CoreServiceProto.UpdateStoreRequest;
import feast.core.CoreServiceProto.UpdateStoreResponse;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.exception.SpecRetrievalException;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory cache of specs.
 */
@Slf4j
public class CachedSpecService {

  private static final int MAX_SPEC_COUNT = 1000;

  private final CoreSpecService coreService;
  private final Path configPath;

  private final CacheLoader<String, FeatureSetSpec> featureSetSpecCacheLoader;
  private final LoadingCache<String, FeatureSetSpec> featureSetSpecCache;
  private Store store;

  public CachedSpecService(CoreSpecService coreService, Path configPath) {
    this.configPath = configPath;
    this.coreService = coreService;
    this.store = updateStore(readConfig(configPath));

    Map<String, FeatureSetSpec> featureSetSpecs = getFeatureSetSpecMap();
    featureSetSpecCacheLoader =
        CacheLoader.from(
            (String key) -> featureSetSpecs.get(key));
    featureSetSpecCache =
        CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(featureSetSpecCacheLoader);
  }

  /**
   * Get the current store configuration.
   *
   * @return StoreProto.Store store configuration for this serving instance
   */
  public Store getStore() {
    return this.store;
  }

  /**
   * Get a single FeatureSetSpec matching the given name and version.
   *
   * @param name of the featureSet
   * @param version to retrieve
   * @return FeatureSetSpec of the matching FeatureSet
   */
  public FeatureSetSpec getFeatureSet(String name, int version) {
    String id = String.format("%s:%d", name, version);
    try {
      return featureSetSpecCache.get(id);
    } catch (InvalidCacheLoadException e) {
      // if not found, try to retrieve from core
      GetFeatureSetsRequest request = GetFeatureSetsRequest.newBuilder()
          .setFilter(Filter.newBuilder()
              .setFeatureSetName(name)
              .setFeatureSetVersion(String.valueOf(version)))
          .build();
      GetFeatureSetsResponse featureSets = coreService.getFeatureSets(request);
      if (featureSets.getFeatureSetsList().size() == 0) {
        throw new SpecRetrievalException(
            String.format(
                "Unable to retrieve featureSet with id %s from core, featureSet does not exist",
                id));
      }
      return featureSets.getFeatureSets(0);
    } catch (ExecutionException e) {
      throw new SpecRetrievalException(
          String.format("Unable to retrieve featureSet with id %s", id), e);
    }
  }

  /**
   * Reload the store configuration from the given config path, then retrieve the necessary specs
   * from core to preload the cache.
   */
  public void populateCache() {
    this.store = updateStore(readConfig(configPath));
    Map<String, FeatureSetSpec> featureSetSpecMap = getFeatureSetSpecMap();
    featureSetSpecCache.putAll(featureSetSpecMap);
  }

  public void scheduledPopulateCache() {
    try {
      populateCache();
    } catch (Exception e) {
      log.warn("Error updating store configuration and specs: {}", e.getMessage());
    }
  }

  private Map<String, FeatureSetSpec> getFeatureSetSpecMap() {
    HashMap<String, FeatureSetSpec> featureSetSpecs = new HashMap<>();

    for (Subscription subscription : this.store.getSubscriptionsList()) {
      try {
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
      } catch (StatusRuntimeException e) {
        throw new RuntimeException(
            String.format("Unable to retrieve specs matching subscription %s", subscription), e);
      }
    }
    return featureSetSpecs;
  }

  private Store readConfig(Path path) {
    try {
      List<String> fileContents = Files.readAllLines(path);
      String yaml = fileContents.stream().reduce("", (l1, l2) -> l1 + "\n" + l2);
      log.info("loaded store config at {}: \n{}", path.toString(), yaml);
      return yamlToStoreProto(yaml);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Unable to read store config at %s", path.toAbsolutePath()), e);
    }
  }

  private Store updateStore(Store store) {
    UpdateStoreRequest request = UpdateStoreRequest.newBuilder().setStore(store).build();
    try {
      UpdateStoreResponse updateStoreResponse = coreService.updateStore(request);
      if (!updateStoreResponse.getStore().equals(store)) {
        throw new RuntimeException("Core store config not matching current store config");
      }
      return updateStoreResponse.getStore();
    } catch (Exception e) {
      throw new RuntimeException("Unable to update store configuration", e);
    }
  }
}
