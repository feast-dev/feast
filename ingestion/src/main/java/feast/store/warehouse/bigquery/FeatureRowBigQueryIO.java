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

package feast.store.warehouse.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import feast.ingestion.model.Specs;
import feast.specs.EntitySpecProto.EntitySpec;
import feast.store.FeatureStoreWrite;
import feast.types.FeatureRowExtendedProto.FeatureRowExtended;
import feast.types.FeatureRowProto.FeatureRow;
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
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;

@Slf4j
public class FeatureRowBigQueryIO {

  public static class Write extends FeatureStoreWrite {

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
      // FeatureRowToBigQueryTableRowDoFn toTableRowDoFn = new FeatureRowToBigQueryTableRowDoFn(specs);
      // BigQueryIO.Write<FeatureRowExtended> write =
      //     BigQueryIO.<FeatureRowExtended>write()
      //         .to(http://dataiku.d.gods.golabs.io
      //             new DynamicDestinations<FeatureRowExtended, String>() {
      //               public String getDestination(ValueInSingleWindow<FeatureRowExtended> element) {
      //                 FeatureRowExtended featureRowExtended = element.getValue();
      //                 FeatureRow row = featureRowExtended.getRow();
      //                 EntitySpec entityInfo = specs.getEntitySpec(row.getEntityName());
      //
      //                 String tableName = entityInfo.getName();
      //                 return bigQueryOptions.project
      //                     + ":"
      //                     + bigQueryOptions.dataset
      //                     + "."
      //                     + tableName;
      //               }
      //
      //               public TableDestination getTable(String tableSpec) {
      //                 return new TableDestination(tableSpec, "Table " + tableSpec);
      //               }
      //
      //               @Override
      //               public TableSchema getSchema(String destination) {
      //                 return null;
      //               }
      //             })
      //         .withFormatFunction(toTableRowDoFn::toTableRow)
      //         .withCreateDisposition(CreateDisposition.CREATE_NEVER)
      //         .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      //         .withMethod(Method.FILE_LOADS);
      // if (!Strings.isNullOrEmpty(bigQueryOptions.tempLocation)) {
      //   log.info(
      //       "Setting customer GCS temp location for BigQuery write to "
      //           + bigQueryOptions.tempLocation);
      //   write =
      //       write.withCustomGcsTempLocation(StaticValueProvider.of(bigQueryOptions.tempLocation));
      // }
      //
      // if (input.isBounded() == IsBounded.UNBOUNDED) {
      //   write =
      //       write
      //           .withTriggeringFrequency(triggerFrequency)
      //           // this is apparently supposed to be the default according to beam code
      //           // comments.
      //           .withNumFileShards(100);
      // }
      // WriteResult result = input.apply(write);
      return PDone.in(input.getPipeline());
    }
  }
}
