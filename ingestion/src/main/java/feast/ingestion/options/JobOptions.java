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

package feast.ingestion.options;

import com.fasterxml.jackson.annotation.JsonProperty;
import feast.options.Options;

/**
 * JobOptions are options passed in via the import spec, they are options that dictate certain
 * behaviour of the job, they differ from the PipelineOptions in ImportJobOptions which are used to
 * influence the execution environment.
 */
public class JobOptions implements Options {

  private long sampleLimit;
  private boolean coalesceRowsEnabled;
  private long coalesceRowsDelaySeconds;
  private long coalesceRowsTimeoutSeconds;


  @JsonProperty(value = "sample.limit")
  public long getSampleLimit() {
    return sampleLimit;
  }

  public void setSampleimit(long sampleLimit) {
    this.sampleLimit = sampleLimit;
  }

  @JsonProperty(value = "coalesceRows.enabled")
  public boolean isCoalesceRowsEnabled() {
    return coalesceRowsEnabled;
  }

  public void setCoalesceRowsEnabled(boolean coalesceRows) {
    this.coalesceRowsEnabled = coalesceRows;
  }

  @JsonProperty(value = "coalesceRows.delaySeconds")
  public long getCoalesceRowsDelaySeconds() {
    return coalesceRowsDelaySeconds;
  }

  public void setCoalesceRowsDelaySeconds(long coalesceRowsDelaySeconds) {
    this.coalesceRowsDelaySeconds = coalesceRowsDelaySeconds;
  }

  @JsonProperty(value = "coalesceRows.timeoutSeconds")
  public long getCoalesceRowsTimeoutSeconds() {
    return coalesceRowsTimeoutSeconds;
  }

  public void setCoalesceRowsTimeoutSeconds(long coalesceRowsTimeoutSeconds) {
    this.coalesceRowsTimeoutSeconds = coalesceRowsTimeoutSeconds;
  }

}
