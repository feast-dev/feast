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
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.core.StoreProto;
import feast.proto.core.StoreProto.Store.BigtableConfig;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import java.util.Map;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

@AutoValue
public abstract class BigtableFeatureSink implements FeatureSink {

  /**
   * Initialize a {@link BigtableFeatureSink.Builder} from a {@link
   * StoreProto.Store.BigtableConfig}.
   *
   * @param bigtableConfig {@link BigtableConfig}
   * @return {@link BigtableFeatureSink.Builder}
   */
  public static FeatureSink fromConfig(BigtableConfig bigtableConfig) {
    return builder().setBigtableConfig(bigtableConfig).build();
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

  PCollectionView<Map<String, Iterable<FeatureSetSpec>>> specsView;

  public BigtableFeatureSink withSpecsView(
      PCollectionView<Map<String, Iterable<FeatureSetSpec>>> specsView) {
    this.specsView = specsView;
    return this;
  }

  PCollectionView<Map<String, Iterable<FeatureSetSpec>>> getSpecsView() {
    return specsView;
  }

  @Override
  public PCollection<FeatureSetReference> prepareWrite(
      PCollection<KV<FeatureSetReference, FeatureSetSpec>> featureSetSpecs) {
    try {
      CloudBigtableTableConfiguration bigtableTableConfig =
          new CloudBigtableTableConfiguration.Builder()
              .withProjectId(getBigtableConfig().getProjectId())
              .withInstanceId(getBigtableConfig().getInstanceId())
              .withTableId(getBigtableConfig().getTableId())
              .build();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to connect to Bigtable with project: '%s' instanceID: '%s'. Please check that your Bigtable is running and accessible from Feast.",
              getBigtableConfig().getProjectId(), getBigtableConfig().getInstanceId()));
    }
    specsView = featureSetSpecs.apply(ParDo.of(new ReferenceToString())).apply(View.asMultimap());
    return featureSetSpecs.apply(Keys.create());
  }

  @Override
  public PTransform<PCollection<FeatureRow>, WriteResult> writer() {
    return new BigtableCustomIO.Write(getBigtableConfig(), getFeatureSetSpecs());
  }

  private static class ReferenceToString
      extends DoFn<KV<FeatureSetReference, FeatureSetSpec>, KV<String, FeatureSetSpec>> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(KV.of(c.element().getKey().getReference(), c.element().getValue()));
    }
  }
}
