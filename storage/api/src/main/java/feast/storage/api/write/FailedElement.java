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
package feast.storage.api.write;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.joda.time.Instant;

@AutoValue
// Use DefaultSchema annotation so this AutoValue class can be serialized by Beam
// https://issues.apache.org/jira/browse/BEAM-1891
// https://github.com/apache/beam/pull/7334
@DefaultSchema(AutoValueSchema.class)
public abstract class FailedElement {
  public abstract Instant getTimestamp();

  @Nullable
  public abstract String getJobName();

  @Nullable
  public abstract String getProjectName();

  @Nullable
  public abstract String getFeatureSetName();

  @Nullable
  public abstract String getFeatureSetVersion();

  @Nullable
  public abstract String getTransformName();

  @Nullable
  public abstract String getPayload();

  @Nullable
  public abstract String getErrorMessage();

  @Nullable
  public abstract String getStackTrace();

  public static Builder newBuilder() {
    return new AutoValue_FailedElement.Builder().setTimestamp(Instant.now());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTimestamp(Instant timestamp);

    public abstract Builder setProjectName(String projectName);

    public abstract Builder setFeatureSetName(String featureSetName);

    public abstract Builder setFeatureSetVersion(String featureSetVersion);

    public abstract Builder setJobName(String jobName);

    public abstract Builder setTransformName(String transformName);

    public abstract Builder setPayload(String payload);

    public abstract Builder setErrorMessage(String errorMessage);

    public abstract Builder setStackTrace(String stackTrace);

    public abstract FailedElement build();
  }
}
