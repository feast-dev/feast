/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.core.job;

import java.util.NoSuchElementException;

/**
 * An Apache Beam Runner, for which Feast Core supports managing ingestion jobs.
 *
 * @see <a href="https://beam.apache.org/documentation/#runners">Beam Runners</a>
 */
public enum Runner {
  DATAFLOW("DataflowRunner"),
  FLINK("FlinkRunner"),
  DIRECT("DirectRunner");

  private final String humanName;

  Runner(String humanName) {
    this.humanName = humanName;
  }

  /** Returns the human readable name of this runner, usable in logging, config files, etc. */
  @Override
  public String toString() {
    return humanName;
  }

  /** Parses a runner from its human readable name. */
  public static Runner fromString(String humanName) {
    for (Runner r : Runner.values()) {
      if (r.toString().equals(humanName)) {
        return r;
      }
    }
    throw new NoSuchElementException("Unknown Runner value: " + humanName);
  }
}
