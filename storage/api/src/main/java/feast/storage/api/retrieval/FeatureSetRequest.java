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
package feast.storage.api.retrieval;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.serving.ServingAPIProto.FeatureReference;
import java.util.List;

@AutoValue
public abstract class FeatureSetRequest {
  public abstract FeatureSetSpec getSpec();

  public abstract ImmutableSet<FeatureReference> getFeatureReferences();

  public static Builder newBuilder() {
    return new AutoValue_FeatureSetRequest.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSpec(FeatureSetSpec spec);

    abstract ImmutableSet.Builder<FeatureReference> featureReferencesBuilder();

    public Builder addAllFeatureReferences(List<FeatureReference> featureReferenceList) {
      featureReferencesBuilder().addAll(featureReferenceList);
      return this;
    }

    public Builder addFeatureReference(FeatureReference featureReference) {
      featureReferencesBuilder().add(featureReference);
      return this;
    }

    public abstract FeatureSetRequest build();
  }
}
