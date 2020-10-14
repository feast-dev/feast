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
package feast.storage.api.retriever;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.types.ValueProto.Value;

@AutoValue
public abstract class Feature {

  public abstract FeatureReferenceV2 getFeatureReference();

  public abstract Value getFeatureValue();

  public abstract Timestamp getEventTimestamp();

  public static Builder builder() {
    return new AutoValue_Feature.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setFeatureReference(FeatureReferenceV2 featureReference);

    public abstract Builder setFeatureValue(Value featureValue);

    public abstract Builder setEventTimestamp(Timestamp eventTimestamp);

    public abstract Feature build();
  }
}
