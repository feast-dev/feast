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

package feast.ingestion.exceptions;

import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import feast.ingestion.values.PFeatureRows;
import feast.types.FeatureRowExtendedProto.Attempt;
import feast.types.FeatureRowExtendedProto.Error;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;

public class ErrorsHandler {

  private ErrorsHandler() {}

  /** Resets the attempt count if that error has changed. */
  public static int checkAttemptCount(int numAttempts, Error lastError, Error thisError) {
    if (lastError != null) {
      if (!lastError.getCause().equals(thisError.getCause())
          || !lastError.getTransform().equals(thisError.getTransform())) {
        return 0;
      }
    }
    return numAttempts;
  }

  public static void handleError(
      ProcessContext context, FeatureRowExtended rowExtended, Error thisError) {
    Attempt lastAttempt = rowExtended.getLastAttempt();
    Error lastError = lastAttempt.getError();

    int numAttempts = checkAttemptCount(lastAttempt.getAttempts(), lastError, thisError);

    Attempt thisAttempt =
        Attempt.newBuilder().setAttempts(numAttempts + 1).setError(thisError).build();

    FeatureRowExtended outputRowExtended =
        FeatureRowExtended.newBuilder().mergeFrom(rowExtended).setLastAttempt(thisAttempt).build();

    context.output(PFeatureRows.ERRORS_TAG, outputRowExtended);
  }
}
