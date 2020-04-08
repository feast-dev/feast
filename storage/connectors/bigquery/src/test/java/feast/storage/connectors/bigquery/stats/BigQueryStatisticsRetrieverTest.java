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
package feast.storage.connectors.bigquery.stats;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.storage.api.statistics.FeatureSetStatistics;
import java.util.Arrays;
import org.junit.Test;

public class BigQueryStatisticsRetrieverTest {

  @Test
  public void shouldRun() throws InvalidProtocolBufferException {
    BigQueryStatisticsRetriever retriever =
        BigQueryStatisticsRetriever.newBuilder()
            .setBigquery(BigQueryOptions.getDefaultInstance().getService())
            .setDatasetId("feast_test_20200202")
            .setProjectId("aliz-development")
            .build();

    //        FeatureSetSpec featureSetSpec = FeatureSetSpec.newBuilder()
    //                .setProject("metrics_test")
    //                .setName("customer_transactions")
    //                .setVersion(1)
    //
    // .addEntities(EntitySpec.newBuilder().setName("customer_id").setValueType(Enum.INT64))
    //
    // .addFeatures(FeatureSpec.newBuilder().setName("total_transactions").setValueType(Enum.INT64))
    //
    // .addFeatures(FeatureSpec.newBuilder().setName("daily_transactions").setValueType(Enum.FLOAT))
    //                .build();

    FeatureSetSpec.Builder featureSetSpec = FeatureSetSpec.newBuilder();
    String bigStatsJson =
        "{\"project\":\"metrics_test\",\"maxAge\":\"345599s\",\"name\":\"big\",\"entities\":[{\"name\":\"driver\",\"valueType\":\"STRING\"}],\"features\":[{\"name\":\"ride_driver_id_num_completed\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_prop_completed\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_distance_completed\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_customer_distance_completed\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_distance_cancelled\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_customer_distance_cancelled\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_standing_completed_1\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_standing_completed_2\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_standing_completed_3\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_standing_cancelled_1\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_standing_cancelled_2\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_standing_cancelled_3\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_origin_completed_1\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_origin_completed_2\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_origin_completed_3\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_origin_cancelled_1\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_origin_cancelled_2\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_origin_cancelled_3\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_destination_completed_1\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_destination_completed_2\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_destination_completed_3\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_destination_cancelled_1\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_destination_cancelled_2\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_destination_cancelled_3\",\"valueType\":\"INT64\"},{\"name\":\"ride_driver_id_donut_count_completed\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_donut_count_cancelled\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_num_completed_recent\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_prop_completed_recent\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_distance_completed_recent\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_customer_distance_completed_recent\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_distance_cancelled_recent\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_avg_customer_distance_cancelled_recent\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_donut_count_completed_recent\",\"valueType\":\"FLOAT\"},{\"name\":\"ride_driver_id_donut_count_cancelled_recent\",\"valueType\":\"FLOAT\"}]}";
    JsonFormat.parser().merge(bigStatsJson, featureSetSpec);
    featureSetSpec.setVersion(1);
    FeatureSetStatistics featureStatistics =
        retriever.getFeatureStatistics(
            featureSetSpec.build(),
            Arrays.asList(
                "ride_driver_id_destination_cancelled_3",
                "ride_driver_id_avg_customer_distance_completed_recent",
                "ride_driver_id_avg_distance_completed_recent",
                "ride_driver_id_origin_cancelled_2",
                "ride_driver_id_donut_count_cancelled",
                "ride_driver_id_destination_completed_3",
                "ride_driver_id_prop_completed",
                "ride_driver_id_origin_completed_2",
                "ride_driver_id_standing_cancelled_1",
                "ride_driver_id_avg_customer_distance_cancelled_recent",
                "ride_driver_id_avg_customer_distance_cancelled",
                "ride_driver_id_donut_count_completed_recent",
                "ride_driver_id_standing_completed_1",
                "ride_driver_id_avg_distance_cancelled_recent",
                "ride_driver_id_destination_cancelled_1",
                "ride_driver_id_standing_cancelled_2",
                "ride_driver_id_origin_completed_3",
                "ride_driver_id_standing_completed_2",
                "ride_driver_id_donut_count_completed",
                "ride_driver_id_origin_cancelled_1",
                "ride_driver_id_origin_cancelled_3",
                "ride_driver_id_origin_completed_1",
                "ride_driver_id_num_completed",
                "ride_driver_id_destination_cancelled_2",
                "ride_driver_id_prop_completed_recent",
                "ride_driver_id_standing_cancelled_3",
                "ride_driver_id_avg_distance_cancelled",
                "ride_driver_id_avg_customer_distance_completed",
                "ride_driver_id_donut_count_cancelled_recent",
                "ride_driver_id_destination_completed_2",
                "ride_driver_id_avg_distance_completed",
                "ride_driver_id_num_completed_recent",
                "ride_driver_id_standing_completed_3",
                "ride_driver_id_destination_completed_1"),
            "dataset");
  }
}
