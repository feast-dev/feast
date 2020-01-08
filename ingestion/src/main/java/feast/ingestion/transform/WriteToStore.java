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
package feast.ingestion.transform;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto.FeatureSet;
import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.ingestion.options.ImportOptions;
import feast.ingestion.utils.ResourceUtil;
import feast.ingestion.values.FailedElement;
import feast.store.serving.bigquery.FeatureRowToTableRow;
import feast.store.serving.bigquery.GetTableDestination;
import feast.store.serving.redis.FeatureRowToRedisMutationDoFn;
import feast.store.serving.redis.RedisCustomIO;
import feast.types.FeatureRowProto.FeatureRow;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;

@AutoValue
public abstract class WriteToStore extends PTransform<PCollection<FeatureRow>, PDone> {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(WriteToStore.class);

  public static final String METRIC_NAMESPACE = "WriteToStore";
  public static final String ELEMENTS_WRITTEN_METRIC = "elements_written";

  private static final Counter elementsWritten =
      Metrics.counter(METRIC_NAMESPACE, ELEMENTS_WRITTEN_METRIC);

  public abstract Store getStore();

  public abstract Map<String, FeatureSet> getFeatureSets();

  public static Builder newBuilder() {
    return new AutoValue_WriteToStore.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStore(Store store);

    public abstract Builder setFeatureSets(Map<String, FeatureSet> featureSets);

    public abstract WriteToStore build();
  }

  @Override
  public PDone expand(PCollection<FeatureRow> input) {
    ImportOptions options = input.getPipeline().getOptions().as(ImportOptions.class);
    StoreType storeType = getStore().getType();

    switch (storeType) {
      case REDIS:
        RedisConfig redisConfig = getStore().getRedisConfig();
        PCollection<FailedElement> redisWriteResult =
            input
                .apply(
                    "FeatureRowToRedisMutation",
                    ParDo.of(new FeatureRowToRedisMutationDoFn(getFeatureSets())))
                .apply("WriteRedisMutationToRedis", RedisCustomIO.write(redisConfig));
        if (options.getDeadLetterTableSpec() != null) {
          redisWriteResult.apply(
              WriteFailedElementToBigQuery.newBuilder()
                  .setTableSpec(options.getDeadLetterTableSpec())
                  .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
                  .build());
        }
        break;
      case BIGQUERY:
        BigQueryConfig bigqueryConfig = getStore().getBigqueryConfig();

        WriteResult bigqueryWriteResult =
            input.apply(
                "WriteTableRowToBigQuery",
                BigQueryIO.<FeatureRow>write()
                    .to(
                        new GetTableDestination(
                            bigqueryConfig.getProjectId(), bigqueryConfig.getDatasetId()))
                    .withFormatFunction(new FeatureRowToTableRow(options.getJobName()))
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withExtendedErrorInfo()
                    .withMethod(Method.STREAMING_INSERTS)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

        if (options.getDeadLetterTableSpec() != null) {
          bigqueryWriteResult
              .getFailedInsertsWithErr()
              .apply(
                  "WrapBigQueryInsertionError",
                  ParDo.of(
                      new DoFn<BigQueryInsertError, FailedElement>() {
                        @ProcessElement
                        public void processElement(ProcessContext context) {
                          InsertErrors error = context.element().getError();
                          TableRow row = context.element().getRow();
                          try {
                            context.output(
                                FailedElement.newBuilder()
                                    .setErrorMessage(error.toPrettyString())
                                    .setPayload(row.toPrettyString())
                                    .setJobName(context.getPipelineOptions().getJobName())
                                    .setTransformName("WriteTableRowToBigQuery")
                                    .build());
                          } catch (IOException e) {
                            log.error(e.getMessage());
                          }
                        }
                      }))
              .apply(
                  WriteFailedElementToBigQuery.newBuilder()
                      .setTableSpec(options.getDeadLetterTableSpec())
                      .setJsonSchema(ResourceUtil.getDeadletterTableSchemaJson())
                      .build());
        }
        break;
      default:
        log.error("Store type '{}' is not supported. No Feature Row will be written.", storeType);
        break;
    }

    input.apply(
        "IncrementWriteToStoreElementsWrittenCounter",
        MapElements.into(TypeDescriptors.booleans())
            .via(
                (FeatureRow row) -> {
                  elementsWritten.inc();
                  return true;
                }));

    return PDone.in(input.getPipeline());
  }
}
