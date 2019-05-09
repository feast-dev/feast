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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.StorageSpecProto.StorageSpec;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonFileStorageManager implements StorageManager {

  public static final String TYPE = "file.json";
  public static final String OPT_FILE_PATH = "path";

  public JsonFileStorageManager(
      StorageSpec storageSpec) {
    Preconditions.checkArgument(storageSpec.getType().equals(TYPE));
    Preconditions
        .checkArgument(
            !Strings.isNullOrEmpty(storageSpec.getOptionsOrDefault(OPT_FILE_PATH, null)));
  }

  /**
   * Update the BigQuery schema of this table given the addition of this feature.
   *
   * @param featureSpec specification of the new feature.
   */
  @Override
  public void registerNewFeature(FeatureSpec featureSpec) {

  }
}
