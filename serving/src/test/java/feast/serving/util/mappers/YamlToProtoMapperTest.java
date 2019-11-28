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
package feast.serving.util.mappers;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

import feast.core.StoreProto.Store;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import java.io.IOException;
import org.junit.Test;

public class YamlToProtoMapperTest {

  @Test
  public void shouldConvertYamlToProto() throws IOException {
    String yaml =
        "name: test\n"
            + "type: REDIS\n"
            + "redis_config:\n"
            + "  host: localhost\n"
            + "  port: 6379\n"
            + "subscriptions:\n"
            + "- name: \"*\"\n"
            + "  version: \">0\"\n";
    Store store = YamlToProtoMapper.yamlToStoreProto(yaml);
    Store expected =
        Store.newBuilder()
            .setName("test")
            .setType(StoreType.REDIS)
            .setRedisConfig(RedisConfig.newBuilder().setHost("localhost").setPort(6379))
            .addSubscriptions(Subscription.newBuilder().setName("*").setVersion(">0"))
            .build();
    assertThat(store, equalTo(expected));
  }
}
