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

package feast.storage;

import feast.ingestion.model.Specs;
import feast.specs.StorageSpecProto.StorageSpec;
import lombok.Getter;

public class MockFeatureStore implements FeatureStore {

  private final String type;
  @Getter private MockTransforms.Write write;

  public MockFeatureStore(String type) {
    this.type = type;
  }

  @Override
  public MockTransforms.Write create(StorageSpec storageSpec, Specs specs) {
    write = new MockTransforms.Write(storageSpec);
    return write;
  }

  @Override
  public String getType() {
    return type;
  }
}
