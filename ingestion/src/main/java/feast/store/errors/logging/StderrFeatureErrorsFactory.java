/*
 * Copyright 2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package feast.store.errors.logging;

import com.google.auto.service.AutoService;
import feast.ingestion.model.Specs;
import feast.store.FeatureStoreWrite;
import feast.specs.StorageSpecProto.StorageSpec;
import feast.store.errors.FeatureErrorsFactory;
import org.slf4j.event.Level;

@AutoService(FeatureErrorsFactory.class)
public class StderrFeatureErrorsFactory implements FeatureErrorsFactory {

  public static final String TYPE_STDERR = "stderr";

  @Override
  public FeatureStoreWrite create(StorageSpec storageSpec, Specs specs) {
    return new LogIO.Write(Level.ERROR);
  }

  @Override
  public String getType() {
    return TYPE_STDERR;
  }
}
