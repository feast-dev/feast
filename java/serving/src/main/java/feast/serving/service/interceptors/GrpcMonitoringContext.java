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
package feast.serving.service.interceptors;

import java.util.Optional;

public class GrpcMonitoringContext {
  private static GrpcMonitoringContext INSTANCE;

  final ThreadLocal<String> project = new ThreadLocal();

  private GrpcMonitoringContext() {}

  public static GrpcMonitoringContext getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new GrpcMonitoringContext();
    }

    return INSTANCE;
  }

  public void setProject(String name) {
    this.project.set(name);
  }

  public Optional<String> getProject() {
    return Optional.ofNullable(this.project.get());
  }

  public void clearProject() {
    this.project.set(null);
  }
}
