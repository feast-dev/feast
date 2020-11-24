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

import static feast.common.models.FeatureTable.getFeatureTableStringRef;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesResponse;
import feast.proto.core.CoreServiceProto.ListProjectsRequest;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureTableProto.FeatureTable;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store;
import feast.proto.serving.ServingAPIProto;
import feast.serving.exception.SpecRetrievalException;
import io.grpc.StatusRuntimeException;
import io.prometheus.client.Gauge;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;

/** In-memory cache of specs hosted in Feast Core. */
public class CachedSpecService {

  private static final int MAX_SPEC_COUNT = 1000;
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(CachedSpecService.class);
  private static final String DEFAULT_PROJECT_NAME = "default";

  private final CoreSpecService coreService;
  private Store store;

  private static Gauge cacheLastUpdated =
      Gauge.build()
          .name("cache_last_updated")
          .subsystem("feast_serving")
          .help("epoch time of the last time the cache was updated")
          .register();

  private final LoadingCache<String, FeatureTableSpec> featureTableCache;
  private static Gauge featureTablesCount =
      Gauge.build()
          .name("feature_table_count")
          .subsystem("feast_serving")
          .help("number of feature tables served by this instance")
          .register();

  private final LoadingCache<
          String, Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2>>
      featureCache;

  public CachedSpecService(CoreSpecService coreService, StoreProto.Store store) {
    this.coreService = coreService;
    this.store = coreService.registerStore(store);

    Map<String, FeatureTableSpec> featureTables = getFeatureTableMap().getLeft();
    CacheLoader<String, FeatureTableSpec> featureTableCacheLoader =
        CacheLoader.from(featureTables::get);
    featureTableCache =
        CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(featureTableCacheLoader);
    featureTableCache.putAll(featureTables);

    Map<String, Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2>> features =
        getFeatureTableMap().getRight();
    CacheLoader<String, Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2>>
        featureCacheLoader = CacheLoader.from(features::get);
    featureCache = CacheBuilder.newBuilder().build(featureCacheLoader);
    featureCache.putAll(features);
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
   * Reload the store configuration from the given config path, then retrieve the necessary specs
   * from core to preload the cache.
   */
  public void populateCache() {
    Map<String, FeatureTableSpec> featureTableMap = getFeatureTableMap().getLeft();

    featureTableCache.invalidateAll();
    featureTableCache.putAll(featureTableMap);

    featureTablesCount.set(featureTableCache.size());

    Map<String, Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2>> featureMap =
        getFeatureTableMap().getRight();
    featureCache.invalidateAll();
    featureCache.putAll(featureMap);

    cacheLastUpdated.set(System.currentTimeMillis());
  }

  public void scheduledPopulateCache() {
    try {
      populateCache();
    } catch (Exception e) {
      log.warn("Error updating store configuration and specs: {}", e.getMessage());
    }
  }

  /**
   * Provides a map for easy retrieval of FeatureTable spec using FeatureTable reference
   *
   * @return Map in the format of <project/featuretable_name: FeatureTableSpec>
   */
  private ImmutablePair<
          Map<String, FeatureTableSpec>,
          Map<String, Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2>>>
      getFeatureTableMap() {
    HashMap<String, FeatureTableSpec> featureTables = new HashMap<>();
    HashMap<String, Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2>> features =
        new HashMap<>();

    List<String> projects =
        coreService.listProjects(ListProjectsRequest.newBuilder().build()).getProjectsList();

    for (String project : projects) {
      try {
        ListFeatureTablesResponse featureTablesResponse =
            coreService.listFeatureTables(
                ListFeatureTablesRequest.newBuilder()
                    .setFilter(ListFeatureTablesRequest.Filter.newBuilder().setProject(project))
                    .build());
        Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2> featureRefSpecMap =
            new HashMap<>();
        for (FeatureTable featureTable : featureTablesResponse.getTablesList()) {
          FeatureTableSpec spec = featureTable.getSpec();
          // Key of Map is in the form of <project/featuretable_name>
          featureTables.put(getFeatureTableStringRef(project, spec), spec);

          String featureTableName = spec.getName();
          List<FeatureProto.FeatureSpecV2> featureSpecs = spec.getFeaturesList();
          for (FeatureProto.FeatureSpecV2 featureSpec : featureSpecs) {
            ServingAPIProto.FeatureReferenceV2 featureReference =
                ServingAPIProto.FeatureReferenceV2.newBuilder()
                    .setFeatureTable(featureTableName)
                    .setName(featureSpec.getName())
                    .build();
            featureRefSpecMap.put(featureReference, featureSpec);
          }
        }
        features.put(project, featureRefSpecMap);
      } catch (StatusRuntimeException e) {
        throw new RuntimeException(
            String.format("Unable to retrieve specs matching project %s", project), e);
      }
    }
    return ImmutablePair.of(featureTables, features);
  }

  public FeatureTableSpec getFeatureTableSpec(
      String project, ServingAPIProto.FeatureReferenceV2 featureReference) {
    String featureTableRefStr = getFeatureTableStringRef(project, featureReference);
    FeatureTableSpec featureTableSpec;
    try {
      featureTableSpec = featureTableCache.get(featureTableRefStr);
    } catch (ExecutionException e) {
      throw new SpecRetrievalException(
          String.format("Unable to find FeatureTable with name: %s", featureTableRefStr), e);
    }

    return featureTableSpec;
  }

  public FeatureProto.FeatureSpecV2 getFeatureSpec(
      String project, ServingAPIProto.FeatureReferenceV2 featureReference) {
    FeatureProto.FeatureSpecV2 featureSpec;
    try {
      Map<ServingAPIProto.FeatureReferenceV2, FeatureProto.FeatureSpecV2> featureRefSpecMap =
          featureCache.get(project);
      featureSpec = featureRefSpecMap.get(featureReference);
    } catch (ExecutionException e) {
      throw new SpecRetrievalException(
          String.format("Unable to find Feature with name: %s", featureReference.getName()), e);
    }

    return featureSpec;
  }
}
