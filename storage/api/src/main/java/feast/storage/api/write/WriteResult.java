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

import feast.types.FeatureRowProto.FeatureRow;
import java.io.Serializable;
import org.apache.beam.sdk.values.PCollection;

/** The result of a write transform. */
public interface WriteResult extends Serializable {

  /**
   * Gets set of successfully written feature rows.
   *
   * @return PCollection of feature rows successfully written to the store
   */
  PCollection<FeatureRow> getSuccessfulInserts();

  /**
   * Gets set of feature rows that were unsuccessfully written to the store. The failed feature rows
   * are wrapped in FailedElement objects so implementations of WriteResult can be flexible in how
   * errors are stored.
   *
   * @return FailedElements of unsuccessfully written feature rows
   */
  PCollection<FailedElement> getFailedInserts();
}
