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

import static feast.common.models.Feature.getFeatureStringWithProjectRef;
import static feast.common.models.FeatureSet.getFeatureSetStringRef;
import static java.util.stream.Collectors.groupingBy;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import feast.proto.core.CoreServiceProto.ListFeatureSetsRequest;
import feast.proto.core.CoreServiceProto.ListFeatureSetsResponse;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.FeatureSetProto.FeatureSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store;
import feast.proto.core.StoreProto.Store.Subscription;
import feast.proto.serving.ServingAPIProto.FeatureReference;
import feast.serving.exception.SpecRetrievalException;
import feast.storage.api.retriever.FeatureSetRequest;
import io.grpc.StatusRuntimeException;
import io.prometheus.client.Gauge;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

/** In-memory cache of specs hosted in Feast Core. */
public class CachedSpecService {

  private static final int MAX_SPEC_COUNT = 1000;
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(CachedSpecService.class);
  private static final String DEFAULT_PROJECT_NAME = "default";
  // flag to signal that multiple featuresets match a specific
  // string feature reference in the feature to featureset mapping.
  private static final String FEATURE_SET_CONFLICT_FLAG = "##CONFLICT##";

  private final CoreSpecService coreService;

  private Map<String, String> featureToFeatureSetMapping;

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
    this.store = coreService.registerStore(store);

    Map<String, FeatureSetSpec> featureSets = getFeatureSetMap();
    featureToFeatureSetMapping =
        new ConcurrentHashMap<>(getFeatureToFeatureSetMapping(featureSets));
    CacheLoader<String, FeatureSetSpec> featureSetCacheLoader = CacheLoader.from(featureSets::get);
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
   * Get FeatureSetSpecs for the given features references. See {@link #getFeatureSets(List,
   * String)} for full documentation.
   */
  public List<FeatureSetRequest> getFeatureSets(List<FeatureReference> featureReferences) {
    return getFeatureSets(featureReferences, "");
  }

  /**
   * Get FeatureSetSpecs for the given features references. If the project is unspecified in the
   * given references or project override, autofills the default project. Throws a {@link
   * SpecRetrievalException} if multiple feature sets match given string reference.
   *
   * @param projectOverride If specified would take the spec request in the context of the given
   *     project only. Otherwise if "", will default to determining the project from the individual
   *     references. Has higher precedence compared to project specifed in Feature Reference if both
   *     are specified.
   * @return FeatureSetRequest containing the specs, and their respective feature references
   */
  public List<FeatureSetRequest> getFeatureSets(
      List<FeatureReference> featureReferences, String projectOverride) {
    List<FeatureSetRequest> featureSetRequests = new ArrayList<>();
    featureReferences.stream()
        .map(
            featureReference -> {
              // apply project override when finding feature set for feature
              FeatureReference queryFeatureRef = featureReference;
              if (!projectOverride.isEmpty()) {
                queryFeatureRef = featureReference.toBuilder().setProject(projectOverride).build();
              }

              String featureSetRefStr = mapFeatureToFeatureSetReference(queryFeatureRef);
              return Pair.of(featureSetRefStr, featureReference);
            })
        .collect(groupingBy(Pair::getLeft))
        .forEach(
            (featureSetRefStr, fsRefStrAndFeatureRefs) -> {
              List<FeatureReference> featureRefs =
                  fsRefStrAndFeatureRefs.stream().map(Pair::getRight).collect(Collectors.toList());
              featureSetRequests.add(buildFeatureSetRequest(featureSetRefStr, featureRefs));
            });
    return featureSetRequests;
  }

  /**
   * Build a Feature Set request from the Feature Set specified by given Feature Set reference and
   * given Feature References.
   *
   * @param featureSetRefStr string feature set reference specifying the feature set that contains
   *     requested features
   * @param featureReferences list of feature references specifying the containing feature set
   *     references.
   */
  private FeatureSetRequest buildFeatureSetRequest(
      String featureSetRefStr, List<FeatureReference> featureReferences) {
    // get feature set for name
    FeatureSetSpec featureSetSpec;
    try {
      featureSetSpec = featureSetCache.get(featureSetRefStr);
    } catch (ExecutionException e) {
      throw new SpecRetrievalException(
          String.format("Unable to find featureSet with name: %s", featureSetRefStr), e);
    }

    // check that requested features reference point to different features in the
    // featureset.
    HashSet<String> featureNames = new HashSet<>();
    featureReferences.forEach(
        ref -> {
          if (featureNames.contains(ref.getName())) {
            throw new SpecRetrievalException(
                "Multiple Feature References referencing the same feature in a featureset is not allowed.");
          }
          featureNames.add(ref.getName());
        });

    return FeatureSetRequest.newBuilder()
        .setSpec(featureSetSpec)
        .addAllFeatureReferences(featureReferences)
        .build();
  }

  /** Maps given Feature Reference to the containing Feature Set's string reference */
  private String mapFeatureToFeatureSetReference(FeatureReference featureReference) {
    // map feature reference to coresponding feature set string reference
    String featureSetRefStr =
        featureToFeatureSetMapping.get(getFeatureStringWithProjectRef(featureReference));
    if (featureSetRefStr == null) {
      throw new SpecRetrievalException(
          String.format(
              "Unable to find Feature Set for the given Feature Reference: %s",
              getFeatureStringWithProjectRef(featureReference)));
    } else if (featureSetRefStr == FEATURE_SET_CONFLICT_FLAG) {
      throw new SpecRetrievalException(
          String.format(
              "Given Feature Reference is amibigous as it matches multiple Feature Sets: %s."
                  + "Please specify a more specific Feature Reference (ie specify the project or feature set)",
              getFeatureStringWithProjectRef(featureReference)));
    }
    return featureSetRefStr;
  }

  /**
   * Reload the store configuration from the given config path, then retrieve the necessary specs
   * from core to preload the cache.
   */
  public void populateCache() {
    Map<String, FeatureSetSpec> featureSetMap = getFeatureSetMap();

    featureSetCache.invalidateAll();
    featureSetCache.putAll(featureSetMap);

    featureToFeatureSetMapping = getFeatureToFeatureSetMapping(featureSetMap);

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
        if (!subscription.getExclude()) {
          ListFeatureSetsResponse featureSetsResponse =
              coreService.listFeatureSets(
                  ListFeatureSetsRequest.newBuilder()
                      .setFilter(
                          ListFeatureSetsRequest.Filter.newBuilder()
                              .setProject(subscription.getProject())
                              .setFeatureSetName(subscription.getName()))
                      .build());

          for (FeatureSet featureSet : featureSetsResponse.getFeatureSetsList()) {
            FeatureSetSpec spec = featureSet.getSpec();
            featureSets.put(getFeatureSetStringRef(spec), spec);
          }
        }
      } catch (StatusRuntimeException e) {
        throw new RuntimeException(
            String.format("Unable to retrieve specs matching subscription %s", subscription), e);
      }
    }
    return featureSets;
  }

  /**
   * Generate a feature to feature set mapping from the given feature sets map. Accounts for
   * variations (missing project, feature_set) in string feature references generated by creating
   * multiple entries in the returned mapping for each variation.
   *
   * @param featureSets map of feature set name to feature set specs
   * @return mapping of string feature references to name of feature sets
   */
  private Map<String, String> getFeatureToFeatureSetMapping(
      Map<String, FeatureSetSpec> featureSets) {
    Map<String, String> mapping = new HashMap<>();

    featureSets.values().stream()
        .forEach(
            featureSetSpec -> {
              for (FeatureSpec featureSpec : featureSetSpec.getFeaturesList()) {
                // Register the different permutations of string feature references
                // that refers to this feature in the feature to featureset mapping.

                // Features in FeatureSets in default project can be referenced without project.
                boolean isInDefaultProject =
                    featureSetSpec.getProject().equals(DEFAULT_PROJECT_NAME);

                for (boolean hasProject : new boolean[] {true, false}) {
                  if (!isInDefaultProject && !hasProject) continue;
                  // Features can be referenced without a featureset if there are no conflicts.
                  for (boolean hasFeatureSet : new boolean[] {true, false}) {
                    // Get mapping between string feature reference and featureset
                    Pair<String, String> singleMapping =
                        this.generateFeatureToFeatureSetMapping(
                            featureSpec, featureSetSpec, hasProject, hasFeatureSet);
                    String featureRef = singleMapping.getKey();
                    String featureSetRef = singleMapping.getValue();
                    // Check if another feature set has already mapped to this
                    // string feature reference. if so mark the conflict.
                    if (mapping.containsKey(featureRef)) {
                      mapping.put(featureRef, FEATURE_SET_CONFLICT_FLAG);
                    } else {
                      mapping.put(featureRef, featureSetRef);
                    }
                  }
                }
              }
            });

    return mapping;
  }

  /**
   * Generate a single mapping between the given feature and the featureset. Maps a feature
   * reference refering to the given feature to the corresponding featureset's name.
   *
   * @param featureSpec specifying the feature to create mapping for.
   * @param featureSetSpec specifying the feature set to create mapping for.
   * @param hasProject whether generated mapping's string feature ref has a project.
   * @param hasFeatureSet whether generated mapping's string feature ref has a featureSet.
   * @return a pair mapping a string feature reference to a featureset name.
   */
  private Pair<String, String> generateFeatureToFeatureSetMapping(
      FeatureSpec featureSpec,
      FeatureSetSpec featureSetSpec,
      boolean hasProject,
      boolean hasFeatureSet) {
    FeatureReference.Builder featureRef =
        FeatureReference.newBuilder()
            .setProject(featureSetSpec.getProject())
            .setFeatureSet(featureSetSpec.getName())
            .setName(featureSpec.getName());
    if (!hasProject) {
      featureRef = featureRef.clearProject();
    }
    if (!hasFeatureSet) {
      featureRef = featureRef.clearFeatureSet();
    }
    return Pair.of(
        getFeatureStringWithProjectRef(featureRef.build()), getFeatureSetStringRef(featureSetSpec));
  }
}
