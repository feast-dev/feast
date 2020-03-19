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
package feast.storage.connectors.bigquery.retrieval;

import static feast.storage.connectors.bigquery.retrieval.BigQueryBatchRetriever.TEMP_TABLE_EXPIRY_DURATION_MS;
import static feast.storage.connectors.bigquery.retrieval.QueryTemplater.generateFullTableName;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.*;
import java.util.concurrent.Callable;

/**
 * Waits for a point-in-time correctness join to complete. On completion, returns a featureSetInfo
 * updated with the reference to the table containing the results of the query.
 */
@AutoValue
public abstract class SubqueryCallable implements Callable<FeatureSetQueryInfo> {

  public abstract BigQuery bigquery();

  public abstract FeatureSetQueryInfo featureSetInfo();

  public abstract Job subqueryJob();

  public static Builder builder() {
    return new AutoValue_SubqueryCallable.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBigquery(BigQuery bigquery);

    public abstract Builder setFeatureSetInfo(FeatureSetQueryInfo featureSetInfo);

    public abstract Builder setSubqueryJob(Job subqueryJob);

    public abstract SubqueryCallable build();
  }

  @Override
  public FeatureSetQueryInfo call() throws BigQueryException, InterruptedException {
    QueryJobConfiguration subqueryConfig;
    subqueryJob().waitFor();
    subqueryConfig = subqueryJob().getConfiguration();
    TableId destinationTable = subqueryConfig.getDestinationTable();

    TableInfo expiry =
        bigquery()
            .getTable(destinationTable)
            .toBuilder()
            .setExpirationTime(System.currentTimeMillis() + TEMP_TABLE_EXPIRY_DURATION_MS)
            .build();
    bigquery().update(expiry);

    String fullTablePath = generateFullTableName(destinationTable);

    return new FeatureSetQueryInfo(featureSetInfo(), fullTablePath);
  }
}
