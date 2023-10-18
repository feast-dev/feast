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
package feast.serving.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import org.junit.Before;
import org.junit.Test;

public class FeaturesTest {

  private FeatureReferenceV2 featureReference;

  @Before
  public void setUp() {
    featureReference =
        FeatureReferenceV2.newBuilder()
            .setFeatureViewName("featuretable_1")
            .setFeatureName("feature1")
            .build();
  }

  @Test
  public void shouldReturnFeatureStringRef() {
    String actualFeatureStringRef = FeatureUtil.getFeatureReference(featureReference);
    String expectedFeatureStringRef = "featuretable_1:feature1";

    assertThat(actualFeatureStringRef, equalTo(expectedFeatureStringRef));
  }
}
