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
package feast.storage.connectors.redis.test;

import java.io.IOException;
import redis.embedded.RedisServer;

public class TestUtil {
  public static class LocalRedis {

    private static RedisServer server;

    /**
     * Start local Redis for used in testing at "localhost"
     *
     * @param port port number
     * @throws IOException if Redis failed to start
     */
    public static void start(int port) throws IOException {
      server = new RedisServer(port);
      server.start();
    }

    public static void stop() {
      if (server != null) {
        server.stop();
      }
    }
  }
}
