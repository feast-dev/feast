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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import feast.proto.core.CoreServiceProto;
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

  private static Gauge featureTablesCount =
      Gauge.build()
          .name("feature_table_count")
          .subsystem("feast_serving")
          .help("number of feature tables served by this instance")
          .register();

  private final LoadingCache<ImmutablePair<String, String>, FeatureTableSpec> featureTableCache;

  private final LoadingCache<
          ImmutablePair<String, ServingAPIProto.FeatureReferenceV2>, FeatureProto.FeatureSpecV2>
      featureCache;

  public CachedSpecService(CoreSpecService coreService, StoreProto.Store store) {
    this.coreService = coreService;
    this.store = coreService.registerStore(store);

    CacheLoader<ImmutablePair<String, String>, FeatureTableSpec> featureTableCacheLoader =
        CacheLoader.from(k -> retrieveSingleFeatureTable(k.getLeft(), k.getRight()));
    featureTableCache =
        CacheBuilder.newBuilder().maximumSize(MAX_SPEC_COUNT).build(featureTableCacheLoader);

    CacheLoader<
            ImmutablePair<String, ServingAPIProto.FeatureReferenceV2>, FeatureProto.FeatureSpecV2>
        featureCacheLoader =
            CacheLoader.from(k -> retrieveSingleFeature(k.getLeft(), k.getRight()));
    featureCache = CacheBuilder.newBuilder().build(featureCacheLoader);
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
    ImmutablePair<
            HashMap<ImmutablePair<String, String>, FeatureTableSpec>,
            HashMap<
                ImmutablePair<String, ServingAPIProto.FeatureReferenceV2>,
                FeatureProto.FeatureSpecV2>>
        specs = getFeatureTableMap();

    featureTableCache.invalidateAll();
    featureTableCache.putAll(specs.getLeft());

    featureTablesCount.set(featureTableCache.size());

    featureCache.invalidateAll();
    featureCache.putAll(specs.getRight());

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
          HashMap<ImmutablePair<String, String>, FeatureTableSpec>,
          HashMap<
              ImmutablePair<String, ServingAPIProto.FeatureReferenceV2>,
              FeatureProto.FeatureSpecV2>>
      getFeatureTableMap() {
    HashMap<ImmutablePair<String, String>, FeatureTableSpec> featureTables = new HashMap<>();
    HashMap<ImmutablePair<String, ServingAPIProto.FeatureReferenceV2>, FeatureProto.FeatureSpecV2>
        features = new HashMap<>();

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
          featureTables.put(ImmutablePair.of(project, spec.getName()), spec);

          String featureTableName = spec.getName();
          List<FeatureProto.FeatureSpecV2> featureSpecs = spec.getFeaturesList();
          for (FeatureProto.FeatureSpecV2 featureSpec : featureSpecs) {
            ServingAPIProto.FeatureReferenceV2 featureReference =
                ServingAPIProto.FeatureReferenceV2.newBuilder()
                    .setFeatureTable(featureTableName)
                    .setName(featureSpec.getName())
                    .build();
            features.put(ImmutablePair.of(project, featureReference), featureSpec);
          }
        }

      } catch (StatusRuntimeException e) {
        throw new RuntimeException(
            String.format("Unable to retrieve specs matching project %s", project), e);
      }
    }
    return ImmutablePair.of(featureTables, features);
  }

  private FeatureTableSpec retrieveSingleFeatureTable(String projectName, String tableName) {
    FeatureTable table =
        coreService
            .getFeatureTable(
                CoreServiceProto.GetFeatureTableRequest.newBuilder()
                    .setProject(projectName)
                    .setName(tableName)
                    .build())
            .getTable();
    return table.getSpec();
  }

  private FeatureProto.FeatureSpecV2 retrieveSingleFeature(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    FeatureTableSpec featureTableSpec =
        getFeatureTableSpec(projectName, featureReference); // don't stress core too much
    if (featureTableSpec == null) {
      return null;
    }
    return featureTableSpec.getFeaturesList().stream()
        .filter(f -> f.getName().equals(featureReference.getName()))
        .findFirst()
        .orElse(null);
  }

  public FeatureTableSpec getFeatureTableSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    FeatureTableSpec featureTableSpec;
    try {
      featureTableSpec =
          featureTableCache.get(ImmutablePair.of(projectName, featureReference.getFeatureTable()));
    } catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
      throw new SpecRetrievalException(
          String.format(
              "Unable to find FeatureTable %s/%s", projectName, featureReference.getFeatureTable()),
          e);
    }

    return featureTableSpec;
  }

  public FeatureProto.FeatureSpecV2 getFeatureSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    FeatureProto.FeatureSpecV2 featureSpec;
    try {
      featureSpec = featureCache.get(ImmutablePair.of(projectName, featureReference));
    } catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
      throw new SpecRetrievalException(
          String.format("Unable to find Feature with name: %s", featureReference.getName()), e);
    }

    return featureSpec;
  }
}
