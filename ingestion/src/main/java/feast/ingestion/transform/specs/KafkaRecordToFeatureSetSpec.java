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

import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.FeatureSetProto;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parse FeatureSetSpec from KafkaRecord. We expect message with:
 *
 * <p>
 *
 * <ul>
 *   <li>key: String - FeatureSet reference
 *   <li>value: FeatureSetSpec serialized in proto
 * </ul>
 */
public class KafkaRecordToFeatureSetSpec
    extends DoFn<KafkaRecord<byte[], byte[]>, KV<String, FeatureSetProto.FeatureSetSpec>> {
  private static final Logger log = LoggerFactory.getLogger(KafkaRecordToFeatureSetSpec.class);

  @ProcessElement
  public void process(ProcessContext c) {
    try {
      FeatureSetProto.FeatureSetSpec featureSetSpec =
          FeatureSetProto.FeatureSetSpec.parseFrom(c.element().getKV().getValue());
      c.output(KV.of(new String(c.element().getKV().getKey()), featureSetSpec));
    } catch (InvalidProtocolBufferException e) {
      log.error(
          String.format(
              "Unable to decode FeatureSetSpec with reference %s", c.element().getKV().getKey()));
    }
  }
}
