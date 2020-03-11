/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.storage.api.write;

import feast.core.FeatureSetProto;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/** Interface for implementing user defined feature sink functionality. */
public interface FeatureSink extends Serializable {

  /**
   * Set up storage backend for write. This method will be called once during pipeline
   * initialisation.
   *
   * <p>Examples when schemas need to be updated:
   *
   * <ul>
   *   <li>when a new entity is registered, a table usually needs to be created
   *   <li>when a new feature is registered, a column with appropriate data type usually needs to be
   *       created
   * </ul>
   *
   * <p>If the storage backend is a key-value or a schema-less database, however, there may not be a
   * need to manage any schemas.
   *
   * @param featureSet Feature set to be written
   */
  void prepareWrite(FeatureSetProto.FeatureSet featureSet);

  /**
   * Get a {@link PTransform} that writes feature rows to the store, and returns a {@link
   * WriteResult} that splits successful and failed inserts to be separately logged.
   *
   * @return {@link PTransform}
   */
  PTransform<PCollection<FeatureRow>, WriteResult> write();
}
