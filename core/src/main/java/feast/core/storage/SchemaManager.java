/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.core.storage;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.base.Preconditions;
import feast.core.config.StorageConfig.StorageSpecs;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaManager {

  private final Map<String, StorageManager> storageRegistry = new ConcurrentHashMap<>();
  private final BigQueryViewTemplater viewTemplater;
  private final StorageSpecs storageSpecs;

  public SchemaManager(BigQueryViewTemplater viewTemplater, StorageSpecs storageSpecs) {
    this.viewTemplater = viewTemplater;
    this.storageSpecs = storageSpecs;
    if (storageSpecs.getServingStorageSpec() != null) {
      registerStorage(storageSpecs.getServingStorageSpec());
    } else {
      log.warn("No serving storage is available from storageSpecs, SchemaManager will skip serving store registration");
    }
    if (storageSpecs.getWarehouseStorageSpec() != null) {
      registerStorage(storageSpecs.getWarehouseStorageSpec());
    } else {
      log.warn("No warehouse storage is available from storageSpecs, SchemaManager will skip warehouse store registration");
    }
  }

  /**
   * Prepare warehouse and serving storage for the feature.
   *
   * @param featureSpec spec of the new feature.
   */
  public void registerFeature(FeatureSpec featureSpec) {
    Preconditions.checkNotNull(storageSpecs.getServingStorageSpec(),
        "Attempted to register feature but no serving storage is configured");
    StorageManager servingStorageManager = storageRegistry
        .get(storageSpecs.getServingStorageSpec().getId());
    Preconditions.checkNotNull(servingStorageManager,
        "Serving storage spec has no associated storage manager");
    servingStorageManager.registerNewFeature(featureSpec);

    if (storageSpecs.getWarehouseStorageSpec() != null) {
      StorageManager warehouseStorageManager = storageRegistry
          .get(storageSpecs.getWarehouseStorageSpec().getId());
      Preconditions.checkNotNull(warehouseStorageManager,
          "Warehouse storage spec has no associated storage manager");
      warehouseStorageManager.registerNewFeature(featureSpec);
    }
  }

  /**
   * Register new storage.
   *
   * @param storageSpec new storage spec.
   */
  public void registerStorage(StorageSpec storageSpec) {
    Map<String, String> options = storageSpec.getOptionsMap();
    String id = storageSpec.getId();
    StorageManager storageManager = null;

    switch (storageSpec.getType()) {
      case BigTableStorageManager.TYPE:
        storageManager = new BigTableStorageManager(storageSpec);
        break;
      case BigQueryStorageManager.TYPE:
        String datasetName = options.get(BigQueryStorageManager.OPT_BIGQUERY_DATASET);
        String bqProjectId = options.get(BigQueryStorageManager.OPT_BIGQUERY_PROJECT);
        BigQuery bigQuery =
            BigQueryOptions.newBuilder().setProjectId(bqProjectId).build().getService();
        storageManager =
            new BigQueryStorageManager(id, bigQuery, bqProjectId, datasetName, viewTemplater);
        break;
      case JsonFileStorageManager.TYPE:
        storageManager = new JsonFileStorageManager(storageSpec);
        break;
      case PostgresStorageManager.TYPE:
        String connectionUri = options.get(PostgresStorageManager.OPT_POSTGRES_URI);
        storageManager = new PostgresStorageManager(id, connectionUri);
        break;
      case RedisStorageManager.TYPE:
        storageManager = new RedisStorageManager(id);
        break;
      default:
        log.warn("Unknown storage type: {} \n {}", storageSpec.getType(), storageSpec);
        return;
    }
    storageRegistry.put(id, storageManager);
  }
}
