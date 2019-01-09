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

package feast.core.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/*
 * Application auto configuration
 */
@Configuration
public class AppConfig {
  @Bean
  public ImportJobDefaults getImportJobDefaults(
      @Value("${feast.jobs.coreUri}") String coreApiUri,
      @Value("${feast.jobs.runner}") String runner,
      @Value("${feast.jobs.options}") String options,
      @Value("${feast.jobs.executable}") String executable,
      @Value("${feast.jobs.errorsStoreType}") String errorsStoreType,
      @Value("${feast.jobs.errorsStoreOptions}") String errorsStoreOptions) {
    return new ImportJobDefaults(
        coreApiUri, runner, options, executable, errorsStoreType, errorsStoreOptions);
  }
}
