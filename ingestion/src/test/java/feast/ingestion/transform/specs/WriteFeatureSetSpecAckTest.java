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
package feast.ingestion.transform.specs;

import com.google.common.collect.ImmutableList;
import feast.common.models.FeatureSetReference;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

public class WriteFeatureSetSpecAckTest {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void shouldSendAckWhenAllSinksReady() {
    TestStream<FeatureSetReference> sink1 =
        TestStream.create(AvroCoder.of(FeatureSetReference.class))
            .addElements(FeatureSetReference.of("project", "fs", 1))
            .addElements(FeatureSetReference.of("project", "fs", 2))
            .addElements(FeatureSetReference.of("project", "fs", 3))
            .advanceWatermarkToInfinity();

    TestStream<FeatureSetReference> sink2 =
        TestStream.create(AvroCoder.of(FeatureSetReference.class))
            .addElements(FeatureSetReference.of("project", "fs_2", 1))
            .addElements(FeatureSetReference.of("project", "fs", 3))
            .advanceWatermarkToInfinity();

    TestStream<FeatureSetReference> sink3 =
        TestStream.create(AvroCoder.of(FeatureSetReference.class))
            .advanceProcessingTime(Duration.standardSeconds(10))
            .addElements(FeatureSetReference.of("project", "fs", 3))
            .advanceWatermarkToInfinity();

    PCollectionList<FeatureSetReference> sinks =
        PCollectionList.of(
            ImmutableList.of(
                p.apply("sink1", sink1), p.apply("sink2", sink2), p.apply("sink3", sink3)));

    PCollection<FeatureSetReference> grouped =
        sinks.apply(Flatten.pCollections()).apply(new WriteFeatureSetSpecAck.PrepareWrite(3));

    PAssert.that(grouped)
        .inOnTimePane(GlobalWindow.INSTANCE)
        .containsInAnyOrder(FeatureSetReference.of("project", "fs", 3));

    p.run();
  }
}
