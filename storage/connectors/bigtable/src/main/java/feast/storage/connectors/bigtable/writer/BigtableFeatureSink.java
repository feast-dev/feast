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
package feast.storage.connectors.bigtable.writer;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import feast.proto.core.FeatureSetProto.FeatureSet;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.BigtableConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class BigtableFeatureSink implements FeatureSink {

  /**
   * Initialize a {@link BigtableFeatureSink.Builder} from a {@link StoreProto.Store.BigtableConfig}.
   *
   * @param bigtableConfig {@link BigtableConfig}
   * @param featureSetSpecs
   * @return {@link BigtableFeatureSink.Builder}
   */
  public static FeatureSink fromConfig(
      BigtableConfig bigtableConfig, Map<String, FeatureSetSpec> featureSetSpecs) {
    return builder().setFeatureSetSpecs(featureSetSpecs).setBigtableConfig(bigtableConfig).build();
  }

  public abstract BigtableConfig getBigtableConfig();

  public abstract Map<String, FeatureSetSpec> getFeatureSetSpecs();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_BigtableFeatureSink.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBigtableConfig(BigtableConfig bigtableConfig);

    public abstract Builder setFeatureSetSpecs(Map<String, FeatureSetSpec> featureSetSpecs);

    public abstract BigtableFeatureSink build();
  }

  @Override
  public void prepareWrite(FeatureSet featureSet) {

    try {
      BigtableDataClient bigtableClient =
              BigtableDataClient.create(getBigtableConfig().getProjectId(), getBigtableConfig().getInstanceId());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to Bigtable with project: '%s' instanceID: '%s'. Please check that your Bigtable is running and accessible from Feast.",
              getBigtableConfig().getProject(), getBigtableConfig().getInstanceId()));
    }
  }

  @Override
  public PTransform<PCollection<FeatureRow>, WriteResult> writer() {
    return new BigtableCustomIO.Write(getBigtableConfig(), getFeatureSetSpecs());
  }
}
