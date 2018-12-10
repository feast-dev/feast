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

package feast.core.storage;

import feast.specs.FeatureSpecProto.FeatureSpec;

public class RedisStorageManager implements StorageManager {
  public static final String TYPE = "redis";

  public static final String OPT_REDIS_HOST = "host";
  public static final String OPT_REDIS_PORT = "port";
  private final String id;

  public RedisStorageManager(String id) {
    this.id = id;
  }

  @Override
  public void registerNewFeature(FeatureSpec featureSpec) {
    // do nothing
  }
}
