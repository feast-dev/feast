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

import com.google.common.collect.ImmutableMap;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

/** The result of a write transform. */
public final class WriteResult implements Serializable, POutput {

  private final Pipeline pipeline;
  private final PCollection<FeatureRow> successfulInserts;
  private final PCollection<FailedElement> failedInserts;

  private static TupleTag<FeatureRow> successfulInsertsTag = new TupleTag<>("successfulInserts");
  private static TupleTag<FailedElement> failedInsertsTupleTag = new TupleTag<>("failedInserts");

  /** Creates a {@link WriteResult} in the given {@link Pipeline}. */
  public static WriteResult in(
      Pipeline pipeline,
      PCollection<FeatureRow> successfulInserts,
      PCollection<FailedElement> failedInserts) {
    return new WriteResult(pipeline, successfulInserts, failedInserts);
  }

  private WriteResult(
      Pipeline pipeline,
      PCollection<FeatureRow> successfulInserts,
      PCollection<FailedElement> failedInserts) {

    this.pipeline = pipeline;
    this.successfulInserts = successfulInserts;
    this.failedInserts = failedInserts;
  }

  /**
   * Gets set of feature rows that were unsuccessfully written to the store. The failed feature rows
   * are wrapped in FailedElement objects so implementations of WriteResult can be flexible in how
   * errors are stored.
   *
   * @return FailedElements of unsuccessfully written feature rows
   */
  public PCollection<FailedElement> getFailedInserts() {
    return failedInserts;
  }

  /**
   * Gets set of successfully written feature rows.
   *
   * @return PCollection of feature rows successfully written to the store
   */
  public PCollection<FeatureRow> getSuccessfulInserts() {
    return successfulInserts;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(
        successfulInsertsTag, successfulInserts, failedInsertsTupleTag, failedInserts);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
