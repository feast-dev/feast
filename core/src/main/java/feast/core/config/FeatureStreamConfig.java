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
package feast.core.config;

import feast.core.config.FeastProperties.StreamProperties;
import feast.core.model.Source;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.SourceType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class FeatureStreamConfig {

  @Autowired
  @Bean
  public Source getDefaultSource(FeastProperties feastProperties) {
    StreamProperties streamProperties = feastProperties.getStream();
    SourceType featureStreamType = SourceType.valueOf(streamProperties.getType().toUpperCase());
    switch (featureStreamType) {
      case KAFKA:
        String bootstrapServers = streamProperties.getOptions().getBootstrapServers();
        String topicName = streamProperties.getOptions().getTopic();

        KafkaSourceConfig sourceConfig =
            KafkaSourceConfig.newBuilder()
                .setBootstrapServers(bootstrapServers)
                .setTopic(topicName)
                .build();
        SourceProto.Source source =
            SourceProto.Source.newBuilder()
                .setType(featureStreamType)
                .setKafkaSourceConfig(sourceConfig)
                .build();
        return Source.fromProto(source, true);
      default:
        throw new RuntimeException("Unsupported source stream, only [KAFKA] is supported");
    }
  }
}
