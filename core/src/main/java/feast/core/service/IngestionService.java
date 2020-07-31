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
package feast.core.service;

import feast.proto.core.CoreServiceProto.IngestFeaturesResponse;
import feast.proto.types.FeatureRowProto.FeatureRow;
import java.util.List;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class IngestionService {

  private final KafkaTemplate<String, FeatureRow> featurePublisher;

  public IngestionService(KafkaTemplate<String, FeatureRow> featurePublisher) {
    this.featurePublisher = featurePublisher;
  }

  public IngestFeaturesResponse ingestFeatures(List<FeatureRow> features) {
    features.forEach(
        feat -> {
          featurePublisher.sendDefault(feat);
        });
    return IngestFeaturesResponse.newBuilder().build();
  }
}
