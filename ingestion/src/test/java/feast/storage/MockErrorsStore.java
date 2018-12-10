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

import com.google.auto.service.AutoService;
import lombok.Getter;
import feast.ingestion.model.Specs;
import feast.specs.StorageSpecProto.StorageSpec;

@AutoService(ErrorsStore.class)
public class MockErrorsStore implements ErrorsStore {
  public static final String MOCK_ERRORS_STORE_TYPE = "MOCK_ERRORS_STORE";

  @Getter private MockTransforms.Write write;

  @Override
  public MockTransforms.Write create(StorageSpec storageSpec, Specs specs) {
    write = new MockTransforms.Write(storageSpec);
    return write;
  }

  @Override
  public String getType() {
    return MOCK_ERRORS_STORE_TYPE;
  }
}
