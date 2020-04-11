/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.specs;

import static feast.serving.util.RefUtil.generateFeatureSetStringRef;
import static feast.serving.util.RefUtil.generateFeatureStringRef;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.groupingBy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import feast.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.core.StoreProto;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.Subscription;
import feast.serving.ServingAPIProto.FeatureReference;
import feast.serving.exception.SpecRetrievalException;
import feast.storage.api.retriever.FeatureSetRequest;
import io.grpc.StatusRuntimeException;
import io.prometheus.client.Gauge;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

/** In-memory cache of specs. */
public class CachedSpecService {

  private static final int MAX_SPEC_COUNT = 1000;
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(CachedSpecService.class);

  private final CoreSpecService coreService;

  private final Map<String, String> featureToFeatureSetMapping;

  private final CacheLoader<String, FeatureSetSpec> featureSetCacheLoader;
  private final LoadingCache<String, FeatureSetSpec> featureSetCache;
  private Store store;

  private static Gauge featureSetsCount =
      Gauge.build()
          .name("feature_set_count")
          .subsystem("feast_serving")
          .help("number of feature sets served by this instance")
          .register();
  private static Gauge cacheLastUpdated =
      Gauge.build()
          .name("cache_last_updated")
          .subsystem("feast_serving")
          .help("epoch time of the last time the cache was updated")
          .register();

  public CachedSpecService(CoreSpecService coreService, StoreProto.Store store) {
    this.coreService = coreService;
    this.store = store;

    Map<String, FeatureSetSpec> featureSets = getFeatureSetMap();
    featureToFeatureSetMapping =
        new ConcurrentHashMap<>(getFeatureToFeatureSetMapping(featureSets));
    featureSetCacheLoader = CacheLoader.from(featureSets::get);
    featureSetCache =
        CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(featureSetCacheLoader);
    featureSetCache.putAll(featureSets);
  }

  /**
   * Get the current store configuration.
   *
   * @return StoreProto.Store store configuration for this serving instance
   */
  public Store getStore() {
    return this.store;
  }

  public FeatureSetSpec getFeatureSetSpec(String featureSetRef) throws ExecutionException {
    return featureSetCache.get(featureSetRef);
  }

  /**
   * Get FeatureSetSpecs for the given features.
   *
   * @return FeatureSetRequest containing the specs, and their respective feature references
   */
  public List<FeatureSetRequest> getFeatureSets(List<FeatureReference> featureReferences) {
    List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
    featureReferences.stream()
        .map(
            featureReference -> {
              String featureSet =
                  featureToFeatureSetMapping.getOrDefault(
                      generateFeatureStringRef(featureReference), "");
              if (featureSet.isEmpty()) {
                throw new SpecRetrievalException(
                    String.format("Unable to retrieve feature %s", featureReference));
              }
              return Pair.of(featureSet, featureReference);
            })
        .collect(groupingBy(Pair::getLeft))
        .forEach(
            (fsName, featureRefs) -> {
              try {
                FeatureSetSpec featureSetSpec = featureSetCache.get(fsName);
                List<FeatureReference> requestedFeatures =
                    featureRefs.stream().map(Pair::getRight).collect(Collectors.toList());
                FeatureSetRequest featureSetRequest =
                    FeatureSetRequest.newBuilder()
                        .setSpec(featureSetSpec)
                        .addAllFeatureReferences(requestedFeatures)
                        .build();
                featureSetRequests.add(featureSetRequest);
              } catch (ExecutionException e) {
                throw new SpecRetrievalException(
                    String.format("Unable to retrieve featureSet with id %s", fsName), e);
              }
            });
    return featureSetRequests;
  }

  /**
   * Reload the store configuration from the given config path, then retrieve the necessary specs
   * from core to preload the cache.
   */
  public void populateCache() {
    Map<String, FeatureSetSpec> featureSetMap = getFeatureSetMap();
    featureSetCache.putAll(featureSetMap);
    featureToFeatureSetMapping.putAll(getFeatureToFeatureSetMapping(featureSetMap));

    featureSetsCount.set(featureSetCache.size());
    cacheLastUpdated.set(System.currentTimeMillis());
  }

  public void scheduledPopulateCache() {
    try {
      populateCache();
    } catch (Exception e) {
      log.warn("Error updating store configuration and specs: {}", e.getMessage());
    }
  }

  private Map<String, FeatureSetSpec> getFeatureSetMap() {
    HashMap<String, FeatureSetSpec> featureSets = new HashMap<>();

    for (Subscription subscription : this.store.getSubscriptionsList()) {
      try {
        ListFeatureSetsResponse featureSetsResponse =
            coreService.listFeatureSets(
                ListFeatureSetsRequest.newBuilder()
                    .setFilter(
                        ListFeatureSetsRequest.Filter.newBuilder()
                            .setProject(subscription.getProject())
                            .setFeatureSetName(subscription.getName())
                            .setFeatureSetVersion(subscription.getVersion()))
                    .build());

        for (FeatureSet featureSet : featureSetsResponse.getFeatureSetsList()) {
          FeatureSetSpec spec = featureSet.getSpec();
          featureSets.put(generateFeatureSetStringRef(spec), spec);
        }
      } catch (StatusRuntimeException e) {
        throw new RuntimeException(
            String.format("Unable to retrieve specs matching subscription %s", subscription), e);
      }
    }
    return featureSets;
  }

  private Map<String, String> getFeatureToFeatureSetMapping(
      Map<String, FeatureSetSpec> featureSets) {
    HashMap<String, String> mapping = new HashMap<>();

    featureSets.values().stream()
        .collect(groupingBy(featureSet -> Pair.of(featureSet.getProject(), featureSet.getName())))
        .forEach(
            (group, groupedFeatureSets) -> {
              groupedFeatureSets =
                  groupedFeatureSets.stream()
                      .sorted(comparingInt(FeatureSetSpec::getVersion))
                      .collect(Collectors.toList());
              for (int i = 0; i < groupedFeatureSets.size(); i++) {
                FeatureSetSpec featureSetSpec = groupedFeatureSets.get(i);
                for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
                  FeatureReference featureRef =
                      FeatureReference.newBuilder()
                          .setProject(featureSetSpec.getProject())
                          .setName(featureSpec.getName())
                          .setVersion(featureSetSpec.getVersion())
                          .build();
                  mapping.put(
                      generateFeatureStringRef(featureRef),
                      generateFeatureSetStringRef(featureSetSpec));
                  if (i == groupedFeatureSets.size() - 1) {
                    featureRef =
                        FeatureReference.newBuilder()
                            .setProject(featureSetSpec.getProject())
                            .setName(featureSpec.getName())
                            .build();
                    mapping.put(
                        generateFeatureStringRef(featureRef),
                        generateFeatureSetStringRef(featureSetSpec));
                  }
                }
              }
            });
    return mapping;
  }
}
