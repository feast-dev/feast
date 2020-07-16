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
package feast.storage.connectors.bigquery.writer;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.*;
import feast.common.models.FeatureSetReference;
import feast.proto.core.FeatureSetProto;
import feast.proto.core.StoreProto.Store.BigQueryConfig;
import feast.proto.types.FeatureRowProto;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

@AutoValue
public abstract class BigQueryFeatureSink implements FeatureSink {
  public abstract String getProjectId();

  public abstract String getDatasetId();

  public abstract Duration getTriggeringFrequency();

  @Nullable
  public abstract BigQueryServices getBQTestServices();

  @Nullable
  public abstract ValueProvider<BigQuery> getBQClient();

  private PCollectionView<Map<String, Iterable<TableSchema>>> schemasView;

  /**
   * Initialize a {@link BigQueryFeatureSink.Builder} from a {@link BigQueryConfig}. This method
   * initializes a {@link BigQuery} client with default options. Use the builder method to inject
   * your own client.
   *
   * @param config {@link BigQueryConfig}
   * @return {@link BigQueryFeatureSink.Builder}
   */
  public static FeatureSink fromConfig(BigQueryConfig config) {
    return BigQueryFeatureSink.builder()
        .setDatasetId(config.getDatasetId())
        .setProjectId(config.getProjectId())
        .setBQTestServices(null)
        .setBQClient(
            new ValueProvider<BigQuery>() {
              @Override
              public BigQuery get() {
                return BigQueryOptions.getDefaultInstance().getService();
              }

              @Override
              public boolean isAccessible() {
                return true;
              }
            })
        .setTriggeringFrequency(
            Duration.standardSeconds(config.getWriteTriggeringFrequencySeconds()))
        .build();
  }

  public static Builder builder() {
    return new AutoValue_BigQueryFeatureSink.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract Builder setTriggeringFrequency(Duration triggeringFrequency);

    public abstract Builder setBQTestServices(BigQueryServices bigQueryServices);

    public abstract Builder setBQClient(ValueProvider<BigQuery> bigQueryOptions);

    public abstract BigQueryFeatureSink build();
  }

  /** @param featureSetSpecs Feature set to be written */
  @Override
  public PCollection<FeatureSetReference> prepareWrite(
      PCollection<KV<FeatureSetReference, FeatureSetProto.FeatureSetSpec>> featureSetSpecs) {
    PCollection<KV<FeatureSetReference, TableSchema>> schemas =
        featureSetSpecs
            .apply(
                "GenerateTableSchema",
                ParDo.of(
                    new FeatureSetSpecToTableSchema(
                        DatasetId.of(getProjectId(), getDatasetId()), getBQClient())))
            .setCoder(
                KvCoder.of(
                    AvroCoder.of(FeatureSetReference.class),
                    FeatureSetSpecToTableSchema.TableSchemaCoder.of()));

    schemasView =
        schemas
            .apply("ReferenceString", ParDo.of(new ReferenceToString()))
            .apply("View", View.asMultimap());

    return schemas.apply("Ready", Keys.create());
  }

  @Override
  public PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> writer() {
    return new BigQueryWrite(DatasetId.of(getProjectId(), getDatasetId()), schemasView)
        .withTriggeringFrequency(getTriggeringFrequency())
        .withTestServices(getBQTestServices());
  }

  private static class ReferenceToString
      extends DoFn<KV<FeatureSetReference, TableSchema>, KV<String, TableSchema>> {
    @ProcessElement
    public void process(ProcessContext c) {
      c.output(KV.of(c.element().getKey().getReference(), c.element().getValue()));
    }
  }
}
