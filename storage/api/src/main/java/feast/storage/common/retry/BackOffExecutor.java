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
package feast.storage.common.retry;

import java.io.Serializable;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;

public class BackOffExecutor implements Serializable {

  private final Integer maxRetries;
  private final Duration initialBackOff;

  public BackOffExecutor(Integer maxRetries, Duration initialBackOff) {
    this.maxRetries = maxRetries;
    this.initialBackOff = initialBackOff;
  }

  public void execute(Retriable retriable) throws Exception {
    FluentBackoff backoff =
        FluentBackoff.DEFAULT.withMaxRetries(maxRetries).withInitialBackoff(initialBackOff);
    execute(retriable, backoff);
  }

  private void execute(Retriable retriable, FluentBackoff backoff) throws Exception {
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff = backoff.backoff();
    while (true) {
      try {
        retriable.execute();
        break;
      } catch (Exception e) {
        if (retriable.isExceptionRetriable(e) && BackOffUtils.next(sleeper, backOff)) {
          retriable.cleanUpAfterFailure();
        } else {
          throw e;
        }
      }
    }
  }
}
