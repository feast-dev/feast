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

package feast.serving.http;

import feast.serving.service.SpecStorage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** HTTP end-point for kubernetes health check. */
@RestController
@Slf4j
public class HealthHttpService {

  private final SpecStorage specStorage;

  @Autowired
  public HealthHttpService(SpecStorage specStorage) {
    this.specStorage = specStorage;
  }

  @RequestMapping("/ping")
  public String ping() {
    return "pong";
  }

  @RequestMapping("/healthz")
  public String healthz() {
    if (specStorage.isConnected()) {
      return "healthy";
    }
    log.error("not ready: unable to connect to core service");
    throw new IllegalStateException("not ready: unable to connect to core service");
  }
}
