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
package feast.storage.connectors.bigquery.statistics;

import com.google.cloud.bigquery.TableId;
import com.google.protobuf.Timestamp;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * StatsDataset represents a subset of data within a table to retrieve statistics over. Data can be
 * subset by either ingestionId or date.
 */
public class StatsDataset {
  private final String table;
  private String ingestionId = "";
  private String date = "";

  public StatsDataset(String project, String bigqueryDataset, String table) {
    this.table = generateTableName(project, bigqueryDataset, table);
  }

  public StatsDataset(TableId tableId) {
    this.table = generateTableName(tableId.getProject(), tableId.getDataset(), tableId.getTable());
  }

  public void subsetByIngestionId(String ingestionId) {
    this.ingestionId = ingestionId;
  }

  public void subsetByDate(Timestamp date) {
    DateTime dateTime = new DateTime(date.getSeconds() * 1000, DateTimeZone.UTC);
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    this.date = fmt.print(dateTime);
  }

  public Map<String, Object> getMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("table", table);
    map.put("ingestionId", ingestionId);
    map.put("date", date);
    return map;
  }

  private String generateTableName(String projectId, String datasetId, String tableName) {
    return String.format("%s.%s.%s", projectId, datasetId, tableName);
  }
}
