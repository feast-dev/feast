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

import java.util.Optional;

@AutoValue
@JsonDeserialize(builder = AutoValue_JarParams.Builder.class)
public abstract class JarParams {

  @JsonProperty("topic_name")
  public abstract Optional<String> getTopicName();

  @JsonProperty("kafka_broker")
  public abstract Optional<String> getKafkaBroker();

  public static Builder builder() {
    return new AutoValue_JarParams.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    @JsonProperty("topic_name")
    public abstract Builder setTopicName(String value);

    @JsonProperty("kafka_broker")
    public abstract Builder setKafkaBroker(String value);

    public abstract JarParams build();
  }
}
