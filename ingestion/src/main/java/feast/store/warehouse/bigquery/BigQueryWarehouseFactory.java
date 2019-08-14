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

package feast.store.warehouse.bigquery;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import feast.ingestion.model.Specs;
import feast.store.FeatureStoreWrite;
import feast.options.OptionsParser;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.warehouse.FeatureWarehouseFactory;

@AutoService(FeatureWarehouseFactory.class)
public class BigQueryWarehouseFactory implements FeatureWarehouseFactory {
  public static final String TYPE_BIGQUERY = "BIGQUERY";

  @Override
  public FeatureStoreWrite create(StorageSpec storageSpec, Specs specs) {
    Preconditions.checkArgument(
        storageSpec.getType().equals(TYPE_BIGQUERY), "Storage spec type was not " + TYPE_BIGQUERY);

    BigQueryStoreOptions options =
        OptionsParser.parse(storageSpec.getOptionsMap(), BigQueryStoreOptions.class);

    return new FeatureRowBigQueryIO.Write(options, specs);
  }

  @Override
  public String getType() {
    return TYPE_BIGQUERY;
  }
}
