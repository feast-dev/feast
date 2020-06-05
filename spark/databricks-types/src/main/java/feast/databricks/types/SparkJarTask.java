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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Optional;

@AutoValue
@JsonDeserialize(builder = AutoValue_SparkJarTask.Builder.class)
public abstract class SparkJarTask {
  @JsonProperty("main_class_name")
  public abstract String getMainClassName();

  @JsonProperty("parameters")
  public abstract Optional<List<String>> getParameters();

  public static Builder builder() {
    return new AutoValue_SparkJarTask.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    @JsonProperty("main_class_name")
    public abstract Builder setMainClassName(String value);

    @JsonProperty("parameters")
    public abstract Builder setParameters(List<String> value);

    public abstract SparkJarTask build();
  }
}
