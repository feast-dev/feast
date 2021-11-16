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
package feast.serving.it;

import static org.junit.jupiter.api.Assertions.*;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import feast.proto.core.RegistryProto;
import java.io.IOException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.registry:src/test/resources/docker-compose/feast10/registry.db",
    })
public class ServingRedisLocalRegistryIT extends ServingBase {

  public static final Logger log = LoggerFactory.getLogger(ServingRedisLocalRegistryIT.class);

  @LocalServerPort private int metricsPort;

  @Override
  void updateRegistryFile(RegistryProto.Registry registry) {}

  @Disabled
  @Override
  public void shouldRefreshRegistryAndServeNewFeatures() throws InterruptedException {}

  @Test
  public void shouldAllowUnauthenticatedAccessToMetricsEndpoint() throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("http://localhost:%d/metrics", metricsPort))
            .get()
            .build();
    Response response = new OkHttpClient().newCall(request).execute();
    assertTrue(response.isSuccessful());
    assertFalse(response.body().string().isEmpty());
  }
}
