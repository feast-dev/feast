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
package feast.core.metrics;

import static org.junit.Assert.assertTrue;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import feast.common.it.BaseIT;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.LocalServerPort;

@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.security.authentication.enabled=true",
    })
public class CoreMetricsIT extends BaseIT {
  private static final String METRIC_ENDPOINT = "/metrics";
  private static OkHttpClient httpClient;
  @LocalServerPort private int metricsPort;

  @BeforeAll
  public static void globalSetUp() {
    httpClient = new OkHttpClient();
  }

  /** Test that Feast Core metrics endpoint can be accessed with authentication enabled */
  @Test
  public void shouldAllowUnauthenticatedAccessToMetricsEndpoint() throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("http://localhost:%d%s", metricsPort, METRIC_ENDPOINT))
            .get()
            .build();
    Response response = httpClient.newCall(request).execute();
    assertTrue(response.isSuccessful());
    assertTrue(!response.body().string().isEmpty());
  }
}
