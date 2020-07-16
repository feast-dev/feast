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
package feast.storage.api.writer;

import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Interface for implementing user defined feature sink functionality. */
public interface FeatureSink extends Serializable {

  /**
   * Set up storage backend for write. This method will be called once during pipeline
   * initialisation.
   *
   * <p>Should create transformation that would update sink's state based on given FeatureSetSpec
   * stream. Returning stream should notify subscribers about successful installation of new
   * FeatureSetSpec referenced by {@link FeatureSetReference}.
   *
   * @param featureSetSpecs specs stream
   * @return stream of state updated events
   */
  PCollection<FeatureSetReference> prepareWrite(
      PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs);

  /**
   * Get a {@link PTransform} that writes feature rows to the store, and returns a {@link
   * WriteResult} that splits successful and failed inserts to be separately logged.
   *
   * @return {@link PTransform}
   */
  PTransform<PCollection<FeatureRow>, WriteResult> writer();
}
