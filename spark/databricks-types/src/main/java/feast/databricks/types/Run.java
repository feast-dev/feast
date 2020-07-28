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
package feast.databricks.types;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.auto.value.AutoValue;

@AutoValue
@JsonDeserialize(builder = AutoValue_Run.Builder.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class Run {
  @JsonProperty("run_id")
  public abstract Long getRunId();

  @JsonProperty("state")
  public abstract RunState getState();

  public static Builder builder() {
    return new AutoValue_Run.Builder();
  }

  @AutoValue.Builder
  @JsonIgnoreProperties(ignoreUnknown = true)
  public abstract static class Builder {
    @JsonProperty("runId")
    public abstract Builder setRunId(Long value);

    @JsonProperty("state")
    public abstract Builder setState(RunState value);

    public abstract Run build();
  }
}
