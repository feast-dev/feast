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

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/** Interface for for implementing user defined deadletter sinks to write failed elements to. */
public interface DeadletterSink {

  /**
   * Set up the deadletter sink for writes. This method will be called once during pipeline
   * initialisation.
   */
  void prepareWrite();

  /**
   * Get a {@link PTransform} that writes a collection of FailedElements to the deadletter sink.
   *
   * @return {@link PTransform}
   */
  PTransform<PCollection<FailedElement>, PDone> write();
}
