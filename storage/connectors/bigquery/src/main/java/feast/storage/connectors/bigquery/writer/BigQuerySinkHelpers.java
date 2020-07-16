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

import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.bigquery.DatasetId;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;

public class BigQuerySinkHelpers {

  public static final String DEFAULT_PROJECT_NAME = "default";

  /**
   * Generating BQ table destination from dataset reference and featuresSet's project and name. If
   * project is undefined "default" would be selected
   *
   * @param dataset {@link DatasetId} reference to bq project and dataset
   * @param featureSetKey Feature Set reference with format &lt;project&gt;/&lt;feature-set-name&gt;
   * @return {@link TableDestination}
   */
  public static TableDestination getTableDestination(DatasetId dataset, String featureSetKey) {
    String[] splitName = featureSetKey.split("/");
    String projectName, setName;

    if (splitName.length == 2) {
      projectName = splitName[0];
      setName = splitName[1];
    } else {
      projectName = DEFAULT_PROJECT_NAME;
      setName = splitName[0];
    }

    TimePartitioning timePartitioning =
        new TimePartitioning()
            .setType("DAY")
            .setField(FeatureRowToTableRow.getEventTimestampColumn());

    return new TableDestination(
        String.format(
            "%s:%s.%s_%s",
            dataset.getProject(),
            dataset.getDataset(),
            projectName.replaceAll("-", "_"),
            setName.replaceAll("-", "_")),
        String.format("Feast table for %s", featureSetKey),
        timePartitioning);
  }
}
