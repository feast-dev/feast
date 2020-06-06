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

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import feast.proto.core.StoreProto;
import feast.proto.types.FeatureRowProto.FeatureRow;
import feast.proto.types.FieldProto.Field;
import feast.storage.common.retry.BackOffExecutor;
import io.grpc.Status;
import java.io.IOException;
import org.joda.time.Duration;

public class BigtableStandaloneIngestionClient implements BigtableIngestionClient {
  private final String projectId;
  private final String instanceId;
  private final String table;
  private final Integer backoffMs;
  private final BackOffExecutor backOffExecutor;
  private BigtableDataClient bigtableclient;
  private static final String METADATA_CF = "metadata";
  private static final String FEATURES_CF = "features";
  private static final ByteString FEATURE_SET_QUALIFIER = ByteString.copyFromUtf8("feature_set");
  private static final ByteString INGESTION_ID_QUALIFIER = ByteString.copyFromUtf8("ingestion_id");
  private static final ByteString EVENT_TIMESTAMP_QUALIFIER =
      ByteString.copyFromUtf8("event_timestamp");

  public BigtableStandaloneIngestionClient(StoreProto.Store.BigtableConfig bigtableConfig) {
    this.projectId = bigtableConfig.getProjectId();
    this.instanceId = bigtableConfig.getInstanceId();
    this.table = bigtableConfig.getTableId();
    this.backoffMs =
        bigtableConfig.getInitialBackoffMs() > 0 ? bigtableConfig.getInitialBackoffMs() : 1;
    this.backOffExecutor =
        new BackOffExecutor(bigtableConfig.getMaxRetries(), Duration.millis(this.backoffMs));
  }

  @Override
  public void setup() {
    try {
      this.bigtableclient = BigtableDataClient.create(projectId, instanceId);
    } catch (IOException e) {
      throw Status.UNAVAILABLE
          .withDescription("Unable to set up the BigtableDataClient")
          .withCause(e)
          .asRuntimeException();
    }
  }

  @Override
  public void shutdown() {
    this.bigtableclient.close();
  }

  @Override
  public BackOffExecutor getBackOffExecutor() {
    return this.backOffExecutor;
  }

  @Override
  public void set(String key, FeatureRow value) {
    System.out.printf("Setting the key: %s", key);
    System.out.printf("value: %s", value);
    RowMutation rowMutation = RowMutation.create(table, key);
    rowMutation.setCell(METADATA_CF, FEATURE_SET_QUALIFIER, value.getFeatureSetBytes());
    rowMutation.setCell(METADATA_CF, INGESTION_ID_QUALIFIER, value.getIngestionIdBytes());
    rowMutation.setCell(
        METADATA_CF, EVENT_TIMESTAMP_QUALIFIER, value.getEventTimestamp().toByteString());
    for (Field field : value.getFieldsList()) {
      rowMutation.setCell(FEATURES_CF, field.getNameBytes(), field.getValue().toByteString());
    }
    this.bigtableclient.mutateRow(rowMutation);
  }
}
