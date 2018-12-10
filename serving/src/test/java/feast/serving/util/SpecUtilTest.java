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

package feast.serving.util;

import feast.specs.FeatureSpecProto.DataStore;
import feast.specs.FeatureSpecProto.DataStores;
import feast.specs.FeatureSpecProto.FeatureSpec;
import org.joda.time.Duration;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class SpecUtilTest {
  @Test
  public void shouldBeAbleToParseIsoDurationProperly() {
      DataStore servingDatastore = DataStore.newBuilder()
              .putOptions("bucketSize", "P1D")
              .build();
      DataStores dataStores = DataStores.newBuilder()
              .setServing(servingDatastore)
              .build();
      FeatureSpec featureSpec = FeatureSpec.newBuilder()
              .setDataStores(dataStores)
              .build();

      Duration duration = SpecUtil.getBucketSize(featureSpec);
      assertThat(duration, equalTo(Duration.standardDays(1)));
  }

  @Test
  public void shouldReturn1HourAsDefault(){
      DataStore servingDatastore = DataStore.newBuilder()
              .build();
      DataStores dataStores = DataStores.newBuilder()
              .setServing(servingDatastore)
              .build();
      FeatureSpec featureSpec = FeatureSpec.newBuilder()
              .setDataStores(dataStores)
              .build();

      Duration duration = SpecUtil.getBucketSize(featureSpec);
      assertThat(duration, equalTo(Duration.standardHours(1)));
  }
}