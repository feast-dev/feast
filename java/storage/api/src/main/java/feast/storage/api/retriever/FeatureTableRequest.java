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
import com.google.common.collect.ImmutableSet;
import feast.proto.core.FeatureTableProto.FeatureTableSpec;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AutoValue
public abstract class FeatureTableRequest {
  public abstract FeatureTableSpec getSpec();

  public abstract ImmutableSet<FeatureReferenceV2> getFeatureReferences();

  public static Builder newBuilder() {
    return new AutoValue_FeatureTableRequest.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setSpec(FeatureTableSpec spec);

    abstract ImmutableSet.Builder<FeatureReferenceV2> featureReferencesBuilder();

    public Builder addAllFeatureReferences(List<FeatureReferenceV2> featureReferenceList) {
      featureReferencesBuilder().addAll(featureReferenceList);
      return this;
    }

    public Builder addFeatureReference(FeatureReferenceV2 featureReference) {
      featureReferencesBuilder().add(featureReference);
      return this;
    }

    public abstract FeatureTableRequest build();
  }

  public Map<String, FeatureReferenceV2> getFeatureRefsByName() {
    return getFeatureReferences().stream()
        .collect(
            Collectors.toMap(
                FeatureReferenceV2::getFeatureName, featureReference -> featureReference));
  }
}
