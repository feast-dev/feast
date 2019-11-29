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

import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.protobuf.ProtobufJsonFormatHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Configuration for the spring web MVC layer */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
  /**
   * Get a json-protobuf converter.
   *
   * @return ProtobufJsonFormatHttpMessageConverter
   */
  @Bean
  ProtobufJsonFormatHttpMessageConverter getProtobufHttpMessageConverter() {
    return new ProtobufJsonFormatHttpMessageConverter();
  }

  /** Register json-protobuf converter. */
  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
    converters.add(getProtobufHttpMessageConverter());
  }
}
