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

import com.google.common.collect.Lists;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import feast.proto.core.StoreProto;
import feast.storage.common.retry.BackOffExecutor;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;

public class BigtableStandaloneIngestionClient implements BigtableIngestionClient {
  private final String projectId;
  private final String instanceId;
  private final String table;
  private final Integer backoffMs;
  private final BackOffExecutor backOffExecutor;
  private BigtableDataClient bigtableclient;
  private static final int DEFAULT_TIMEOUT = 2000;

  public BigtableStandaloneIngestionClient(StoreProto.Store.BigtableConfig bigtableConfig) {
    this.projectId = bigtableConfig.getProjectId();
    this.instanceId = bigtableConfig.getInstanceId();
    this.table = bigtableConfig.getTableId();
    long backoffMs = bigtableConfig.getInitialBackoffMs() > 0 ? bigtableConfig.getInitialBackoffMs() : 1;
    this.backOffExecutor =
            new BackOffExecutor(bigtableConfig.getMaxRetries(), Duration.millis(this.backoffMs));
  }

  @Override
  public void setup() {
    this.bigtableclient = BigtableDataClient.create(projectId, instanceId);
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
  public void set(String key, ByteString value) {
    RowMutation rowMutation =
            RowMutation.create(table, key)
                    .setCell("feature", "", value);
    this.bigtableclient.mutateRow(rowMutation);
  }

}
