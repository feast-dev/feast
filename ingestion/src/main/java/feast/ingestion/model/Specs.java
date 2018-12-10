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

package feast.ingestion.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import feast.ingestion.service.SpecRetrievalException;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.specs.StorageSpecProto.StorageSpec;

public interface Specs extends Serializable {
  FeatureSpec getFeatureSpec(String featureId);

  List<FeatureSpec> getFeatureSpecByServingStoreId(String storeId) throws SpecRetrievalException;

  EntitySpec getEntitySpec(String entityName) throws SpecRetrievalException;

  ImportSpec getImportSpec() throws SpecRetrievalException;

  Map<String, StorageSpec> getStorageSpecs() throws SpecRetrievalException;

  StorageSpec getStorageSpec(String storeId);

  String getJobName();
}
