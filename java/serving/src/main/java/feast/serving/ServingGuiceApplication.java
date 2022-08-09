/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package feast.serving;

import com.google.inject.Guice;
import com.google.inject.Injector;
import feast.serving.service.config.*;
import io.grpc.Server;
import java.io.IOException;

public class ServingGuiceApplication {

  public static void main(String[] args) throws InterruptedException, IOException {
    if (args.length == 0) {
      throw new RuntimeException(
          "Path to application configuration file needs to be specified via CLI");
    }

    final Injector i =
        Guice.createInjector(
            new ServingServiceV2Module(),
            new RegistryConfigModule(),
            new InstrumentationConfigModule(),
            new ServerModule(),
            new ApplicationPropertiesModule(args));

    Server server = i.getInstance(Server.class);

    server.start();
    server.awaitTermination();
  }
}
