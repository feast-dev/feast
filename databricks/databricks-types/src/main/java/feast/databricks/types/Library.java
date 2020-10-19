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

@AutoValue
@JsonDeserialize(builder = AutoValue_Library.Builder.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class Library {
  @JsonProperty("jar")
  public abstract String getJar();

  public static Builder builder() {
    return new AutoValue_Library.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    @JsonProperty("jar")
    public abstract Builder setJar(String value);

    public abstract Library build();
  }
}
