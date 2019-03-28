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

package feast.store.serving.bigtable;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import feast.ingestion.model.Specs;
import feast.store.FeatureStoreWrite;
import feast.options.OptionsParser;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.serving.FeatureServingFactory;

@AutoService(FeatureServingFactory.class)
public class BigTableServingStoreFactory implements FeatureServingFactory {

  public static String TYPE_BIGTABLE = "bigtable";

  @Override
  public FeatureStoreWrite create(StorageSpec storageSpec, Specs specs) {
    Preconditions.checkArgument(storageSpec.getType().equals(getType()));

    BigTableStoreOptions options =
        OptionsParser.parse(storageSpec.getOptionsMap(), BigTableStoreOptions.class);
    return new FeatureRowBigTableIO.Write(options, specs);
  }

  @Override
  public String getType() {
    return TYPE_BIGTABLE;
  }
}
