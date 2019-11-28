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

import static feast.types.ValueProto.ValueType.Enum.INT32;
import static feast.types.ValueProto.ValueType.Enum.STRING_LIST;

import com.google.cloud.bigquery.BigQuery;
import feast.core.FeatureSetProto.EntitySpec;
import feast.core.FeatureSetProto.FeatureSetSpec;
import feast.core.FeatureSetProto.FeatureSpec;
import feast.ingestion.utils.StoreUtil;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreUtilTest {
  @Test
  public void setupBigQuery_shouldCreateTable_givenFeatureSetSpec() {
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
}
