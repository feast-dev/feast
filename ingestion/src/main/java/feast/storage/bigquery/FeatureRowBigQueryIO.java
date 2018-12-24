/*
 * Copyright 2018 The Feast Authors
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
 *
 */

package feast.storage.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import feast.ingestion.model.Specs;
import feast.ingestion.transform.FeatureIO;
import feast.ingestion.transform.SplitFeatures.SingleOutputSplit;
import feast.options.OptionsParser;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.specs.FeatureSpecProto.FeatureSpec;
import feast.specs.ImportSpecProto.ImportSpec;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
import feast.types.GranularityProto.Granularity;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;

@Slf4j
public class FeatureRowBigQueryIO {

  public static Read read(ImportSpec importSpec) {
    // TODO: Allow and convert from /projects/{project}/datasets/{dataset}/tables/{table}, to
    // {project}:{dataset}.{table}
    return new Read(importSpec);
  }

  /**
   * Transform for processing BigQuery tables and producing FeatureRow messages.
   *
   * <p>This transform asserts that the import spec is for only one entity, as all columns must have
   * the same entity. The columns names in the import spec will be used for selecting columns from
   * the BigQuery table, but it still scans the whole row.
   *
   * <p>The output is a PCollection of {@link feast.types.FeatureRowProto.FeatureRow FeatureRows},
   * where each feature and the entity key {@link feast.types.ValueProto.Value Value} in the
   * FeatureRow is taken from a column in the BigQuery table and set with the closest type to the
   * BigQuery column schema that is available in {@link feast.types.ValueProto.ValueType ValueType}.
   *
   * <p>Note a small gotcha is that because Integers and Numerics in BigQuery are 64 bits, these are
   * always cast to INT64 and DOUBLE respectively.
   *
   * <p>Downstream these will fail validation if the corresponding {@link
   * feast.specs.FeatureSpecProto.FeatureSpec FeatureSpec} has a 32 bit type.
   */
  @AllArgsConstructor
  public static class Read extends FeatureIO.Read {

    private final ImportSpec importSpec;

    @Override
    public PCollection<FeatureRow> expand(PInput input) {
      Preconditions.checkArgument(importSpec.getType().equals("bigquery"));
      BigQuerySourceOptions options =
          OptionsParser.parse(importSpec.getOptionsMap(), BigQuerySourceOptions.class);

      String url = String.format("%s:%s.%s", options.project, options.dataset, options.table);

      Preconditions.checkArgument(
          importSpec.getEntitiesCount() == 1, "BigQuery read must have only one entity");
      return input
          .getPipeline()
          .apply(
              BigQueryIO.read(new FeatureRowFromBigQuerySchemaAndRecordFn(importSpec)).from(url));
    }
  }

  public static class Write extends FeatureIO.Write {

    private final BigQueryStoreOptions bigQueryOptions;
    private final Specs specs;

    private Duration triggerFrequency = Duration.standardMinutes(5);

    @Inject
    public Write(BigQueryStoreOptions bigQueryOptions, Specs specs) {
      this.bigQueryOptions = bigQueryOptions;
      this.specs = specs;
    }

    @Override
    public PDone expand(PCollection<FeatureRowExtended> input) {
      SingleOutputSplit<Enum> granularitySplitter =
          new SingleOutputSplit<>(FeatureSpec::getGranularity, specs);
      PCollection<FeatureRowExtended> features =
          input.apply("Split by granularity", granularitySplitter);

      FeatureRowToBigQueryTableRowDoFn toTableRowDoFn = new FeatureRowToBigQueryTableRowDoFn(specs);
      BigQueryIO.Write<FeatureRowExtended> write =
          BigQueryIO.<FeatureRowExtended>write()
              .to(
                  new DynamicDestinations<FeatureRowExtended, String>() {
                    public String getDestination(ValueInSingleWindow<FeatureRowExtended> element) {
                      FeatureRowExtended featureRowExtended = element.getValue();
                      FeatureRow row = featureRowExtended.getRow();
                      EntitySpec entityInfo = specs.getEntitySpec(row.getEntityName());

                      Granularity.Enum granularity = row.getGranularity();
                      String tableName =
                          entityInfo.getName() + "_" + granularity.name().toLowerCase();
                      return bigQueryOptions.project
                          + ":"
                          + bigQueryOptions.dataset
                          + "."
                          + tableName;
                    }

                    public TableDestination getTable(String tableSpec) {
                      return new TableDestination(tableSpec, "Table " + tableSpec);
                    }

                    @Override
                    public TableSchema getSchema(String destination) {
                      return null;
                    }
                  })
              .withFormatFunction(toTableRowDoFn::toTableRow)
              .withCreateDisposition(CreateDisposition.CREATE_NEVER)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .withMethod(Method.FILE_LOADS);
      if (!Strings.isNullOrEmpty(bigQueryOptions.tempLocation)) {
        log.info(
            "Setting customer GCS temp location for BigQuery write to "
                + bigQueryOptions.tempLocation);
        write =
            write.withCustomGcsTempLocation(StaticValueProvider.of(bigQueryOptions.tempLocation));
      }
      switch (input.isBounded()) {
        case UNBOUNDED:
          write =
              write
                  .withTriggeringFrequency(triggerFrequency)
                  // this is apparently supposed to be the default according to beam code comments.
                  .withNumFileShards(100);
      }
      WriteResult result = features.apply(write);

      return PDone.in(input.getPipeline());
    }
  }
}
