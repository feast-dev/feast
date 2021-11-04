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
package feast.serving.util;

import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import org.junit.Test;

public class RequestHelperTest {

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfEntityRowEmpty() {
    FeatureReferenceV2 featureReference =
        FeatureReferenceV2.newBuilder()
            .setFeatureTable("featuretablename")
            .setName("featurename")
            .build();
    GetOnlineFeaturesRequestV2 getOnlineFeaturesRequestV2 =
        GetOnlineFeaturesRequestV2.newBuilder().addFeatures(featureReference).build();
    RequestHelper.validateOnlineRequest(getOnlineFeaturesRequestV2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfFeatureReferenceTableEmpty() {
    FeatureReferenceV2 featureReference =
        FeatureReferenceV2.newBuilder().setName("featurename").build();
    GetOnlineFeaturesRequestV2 getOnlineFeaturesRequestV2 =
        GetOnlineFeaturesRequestV2.newBuilder().addFeatures(featureReference).build();
    RequestHelper.validateOnlineRequest(getOnlineFeaturesRequestV2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldErrorIfFeatureReferenceNameEmpty() {
    FeatureReferenceV2 featureReference =
        FeatureReferenceV2.newBuilder().setFeatureTable("featuretablename").build();
    GetOnlineFeaturesRequestV2 getOnlineFeaturesRequestV2 =
        GetOnlineFeaturesRequestV2.newBuilder().addFeatures(featureReference).build();
    RequestHelper.validateOnlineRequest(getOnlineFeaturesRequestV2);
  }
}
