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
package feast.serving.connectors.redis.common;

import feast.proto.serving.ServingAPIProto;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisKeyGenerator {

  public static List<RedisProto.RedisKeyV2> buildRedisKeys(
      String project, List<Map<String, ValueProto.Value>> entityRows) {
    List<RedisProto.RedisKeyV2> redisKeys =
        entityRows.stream()
            .map(entityRow -> makeRedisKey(project, entityRow))
            .collect(Collectors.toList());

    return redisKeys;
  }

  /**
   * Create {@link RedisProto.RedisKeyV2}
   *
   * @param project Project where request for features was called from
   * @param entityRow {@link ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow}
   * @return {@link RedisProto.RedisKeyV2}
   */
  private static RedisProto.RedisKeyV2 makeRedisKey(
      String project, Map<String, ValueProto.Value> entityRow) {
    RedisProto.RedisKeyV2.Builder builder = RedisProto.RedisKeyV2.newBuilder().setProject(project);
    List<String> entityNames = new ArrayList<>(new HashSet<>(entityRow.keySet()));

    // Sort entity names by alphabetical order
    entityNames.sort(String::compareTo);

    for (String entityName : entityNames) {
      builder.addEntityNames(entityName);
      builder.addEntityValues(entityRow.get(entityName));
    }
    return builder.build();
  }
}
