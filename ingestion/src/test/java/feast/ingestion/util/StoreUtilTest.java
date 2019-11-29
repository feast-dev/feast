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
package feast.ingestion.util;

import static feast.types.ValueProto.ValueType.Enum.BOOL;
import static feast.types.ValueProto.ValueType.Enum.BOOL_LIST;
import static feast.types.ValueProto.ValueType.Enum.BYTES;
import static feast.types.ValueProto.ValueType.Enum.BYTES_LIST;
import static feast.types.ValueProto.ValueType.Enum.DOUBLE;
import static feast.types.ValueProto.ValueType.Enum.DOUBLE_LIST;
import static feast.types.ValueProto.ValueType.Enum.FLOAT;
import static feast.types.ValueProto.ValueType.Enum.FLOAT_LIST;
import static feast.types.ValueProto.ValueType.Enum.INT32;
import static feast.types.ValueProto.ValueType.Enum.INT32_LIST;
import static feast.types.ValueProto.ValueType.Enum.INT64;
import static feast.types.ValueProto.ValueType.Enum.INT64_LIST;
import static feast.types.ValueProto.ValueType.Enum.STRING;
import static feast.types.ValueProto.ValueType.Enum.STRING_LIST;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.ingestion.utils.StoreUtil;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreUtilTest {

  @Test
  public void setupBigQuery_shouldCreateTable_givenValidFeatureSetSpec() {
    FeatureSetSpec featureSetSpec =
        FeatureSetSpec.newBuilder()
            .setName("feature_set_1")
            .setVersion(1)
            .addEntities(EntitySpec.newBuilder().setName("entity_1").setValueType(INT32))
            .addFeatures(FeatureSpec.newBuilder().setName("feature_1").setValueType(INT32))
            .addFeatures(FeatureSpec.newBuilder().setName("feature_2").setValueType(STRING_LIST))
            .build();
    BigQuery mockedBigquery = Mockito.mock(BigQuery.class);
    StoreUtil.setupBigQuery(featureSetSpec, "project-1", "dataset_1", mockedBigquery);
  }

  @Test
  public void createBigQueryTableDefinition_shouldCreateCorrectSchema_givenValidFeatureSetSpec() {
    FeatureSetSpec input = FeatureSetSpec.newBuilder().addAllFeatures(Arrays.asList(
        FeatureSpec.newBuilder().setName("bytes_feature").setValueType(BYTES).build(),
        FeatureSpec.newBuilder().setName("string_feature").setValueType(STRING).build(),
        FeatureSpec.newBuilder().setName("int32_feature").setValueType(INT32).build(),
        FeatureSpec.newBuilder().setName("int64_feature").setValueType(INT64).build(),
        FeatureSpec.newBuilder().setName("double_feature").setValueType(DOUBLE).build(),
        FeatureSpec.newBuilder().setName("float_feature").setValueType(FLOAT).build(),
        FeatureSpec.newBuilder().setName("bool_feature").setValueType(BOOL).build(),
        FeatureSpec.newBuilder().setName("bytes_list_feature").setValueType(BYTES_LIST).build(),
        FeatureSpec.newBuilder().setName("string_list_feature").setValueType(STRING_LIST).build(),
        FeatureSpec.newBuilder().setName("int32_list_feature").setValueType(INT32_LIST).build(),
        FeatureSpec.newBuilder().setName("int64_list_feature").setValueType(INT64_LIST).build(),
        FeatureSpec.newBuilder().setName("double_list_feature").setValueType(DOUBLE_LIST).build(),
        FeatureSpec.newBuilder().setName("float_list_feature").setValueType(FLOAT_LIST).build(),
        FeatureSpec.newBuilder().setName("bool_list_feature").setValueType(BOOL_LIST).build()
    )).build();

    Schema actual = StoreUtil.createBigQueryTableDefinition(input).getSchema();

    Schema expected = Schema.of(Arrays.asList(
        Field.newBuilder("bytes_feature", StandardSQLTypeName.BYTES).build(),
        Field.newBuilder("string_feature", StandardSQLTypeName.STRING).build(),
        Field.newBuilder("int32_feature", StandardSQLTypeName.INT64).build(),
        Field.newBuilder("int64_feature", StandardSQLTypeName.INT64).build(),
        Field.newBuilder("double_feature", StandardSQLTypeName.FLOAT64).build(),
        Field.newBuilder("float_feature", StandardSQLTypeName.FLOAT64).build(),
        Field.newBuilder("bool_feature", StandardSQLTypeName.BOOL).build(),
        Field.newBuilder("bytes_list_feature", StandardSQLTypeName.BYTES).setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("string_list_feature", StandardSQLTypeName.STRING).setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("int32_list_feature", StandardSQLTypeName.INT64).setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("int64_list_feature", StandardSQLTypeName.INT64).setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("double_list_feature", StandardSQLTypeName.FLOAT64).setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("float_list_feature", StandardSQLTypeName.FLOAT64).setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("bool_list_feature", StandardSQLTypeName.BOOL).setMode(Mode.REPEATED)
            .build(),
        // Reserved fields
        Field.newBuilder("event_timestamp", StandardSQLTypeName.TIMESTAMP)
            .setDescription(StoreUtil.BIGQUERY_EVENT_TIMESTAMP_FIELD_DESCRIPTION).build(),
        Field.newBuilder("created_timestamp", StandardSQLTypeName.TIMESTAMP)
            .setDescription(StoreUtil.BIGQUERY_CREATED_TIMESTAMP_FIELD_DESCRIPTION).build(),
        Field.newBuilder("job_id", StandardSQLTypeName.STRING)
            .setDescription(StoreUtil.BIGQUERY_JOB_ID_FIELD_DESCRIPTION).build()
    ));

    Assert.assertEquals(expected, actual);
  }
}
