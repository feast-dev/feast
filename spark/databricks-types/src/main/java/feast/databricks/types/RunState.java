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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.auto.value.AutoValue;
import java.util.Optional;

@AutoValue
@JsonDeserialize(builder = AutoValue_RunState.Builder.class)
public abstract class RunState {

  @JsonProperty("life_cycle_state")
  public abstract RunLifeCycleState getLifeCycleState();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("result_state")
  public abstract Optional<RunResultState> getResultState();

  @JsonProperty("state_message")
  public abstract String getStateMessage();

  public static Builder builder() {
    return new AutoValue_RunState.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    @JsonProperty("life_cycle_state")
    public abstract Builder setLifeCycleState(RunLifeCycleState value);

    @JsonProperty("result_state")
    public abstract Builder setResultState(RunResultState value);

    @JsonProperty("state_message")
    public abstract Builder setStateMessage(String value);

    public abstract RunState build();
  }
}
