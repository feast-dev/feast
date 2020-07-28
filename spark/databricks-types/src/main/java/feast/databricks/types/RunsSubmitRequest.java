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
import java.util.List;
import java.util.Optional;

@AutoValue
@JsonDeserialize(builder = AutoValue_RunsSubmitRequest.Builder.class)
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public abstract class RunsSubmitRequest implements JobDefinition {

  @JsonProperty("new_cluster")
  public abstract NewCluster getNewCluster();

  @JsonProperty("spark_jar_task")
  public abstract SparkJarTask getSparkJarTask();

  @JsonProperty("name")
  public abstract Optional<String> getName();

  @JsonProperty("libraries")
  public abstract Optional<List<Library>> getLibraries();

  @JsonProperty("timeout_seconds")
  public abstract Optional<Integer> getTimeoutSeconds();

  @JsonProperty("idempotency_token")
  public abstract Optional<String> getIdempotencyToken();

  public static Builder builder() {
    return new AutoValue_RunsSubmitRequest.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    @JsonProperty("new_cluster")
    public abstract Builder setNewCluster(NewCluster value);

    @JsonProperty("spark_jar_task")
    public abstract Builder setSparkJarTask(SparkJarTask value);

    @JsonProperty("name")
    public abstract Builder setName(String value);

    @JsonProperty("libraries")
    public abstract Builder setLibraries(List<Library> value);

    @JsonProperty("timeout_seconds")
    public abstract Builder setTimeoutSeconds(Integer value);

    @JsonProperty("idempotency_token")
    public abstract Builder setIdempotencyToken(String value);

    public abstract RunsSubmitRequest build();
  }
}
