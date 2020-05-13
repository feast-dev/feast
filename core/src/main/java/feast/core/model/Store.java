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
package feast.core.model;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.StoreProto;
import feast.core.StoreProto.Store.BigQueryConfig;
import feast.core.StoreProto.Store.Builder;
import feast.core.StoreProto.Store.CassandraConfig;
import feast.core.StoreProto.Store.RedisClusterConfig;
import feast.core.StoreProto.Store.RedisConfig;
import feast.core.StoreProto.Store.StoreType;
import feast.core.StoreProto.Store.Subscription;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@Entity
@Table(name = "stores")
public class Store {

  // Name of the store. Must be unique
  @Id
  @Column(name = "name", nullable = false, unique = true)
  private String name;

  // Type of the store, should map to feast.core.Store.StoreType
  @Column(name = "type", nullable = false)
  private String type;

  // Connection string to the database
  @Column(name = "config", nullable = false)
  @Lob
  private byte[] config;

  // FeatureSets this store is subscribed to, comma delimited.
  @Column(name = "subscriptions")
  private String subscriptions;

  public Store() {
    super();
  }

  public static Store fromProto(StoreProto.Store storeProto) throws IllegalArgumentException {
    List<String> subs = new ArrayList<>();
    for (Subscription s : storeProto.getSubscriptionsList()) {
      subs.add(convertSubscriptionToString(s));
    }
    byte[] config;
    switch (storeProto.getType()) {
      case REDIS:
        config = storeProto.getRedisConfig().toByteArray();
        break;
      case BIGQUERY:
        config = storeProto.getBigqueryConfig().toByteArray();
        break;
      case CASSANDRA:
        config = storeProto.getCassandraConfig().toByteArray();
        break;
      case REDIS_CLUSTER:
        config = storeProto.getRedisClusterConfig().toByteArray();
        break;
      default:
        throw new IllegalArgumentException("Invalid store provided");
    }
    return new Store(
        storeProto.getName(), storeProto.getType().toString(), config, String.join(",", subs));
  }

  public StoreProto.Store toProto() throws InvalidProtocolBufferException {
    List<Subscription> subscriptionProtos = getSubscriptions();
    Builder storeProtoBuilder =
        StoreProto.Store.newBuilder()
            .setName(name)
            .setType(StoreType.valueOf(type))
            .addAllSubscriptions(subscriptionProtos);
    switch (StoreType.valueOf(type)) {
      case REDIS:
        RedisConfig redisConfig = RedisConfig.parseFrom(config);
        return storeProtoBuilder.setRedisConfig(redisConfig).build();
      case BIGQUERY:
        BigQueryConfig bqConfig = BigQueryConfig.parseFrom(config);
        return storeProtoBuilder.setBigqueryConfig(bqConfig).build();
      case CASSANDRA:
        CassandraConfig cassConfig = CassandraConfig.parseFrom(config);
        return storeProtoBuilder.setCassandraConfig(cassConfig).build();
      case REDIS_CLUSTER:
        RedisClusterConfig redisClusterConfig = RedisClusterConfig.parseFrom(config);
        return storeProtoBuilder.setRedisClusterConfig(redisClusterConfig).build();
      default:
        throw new InvalidProtocolBufferException("Invalid store set");
    }
  }

  public List<Subscription> getSubscriptions() {
    return Arrays.stream(subscriptions.split(","))
        .map(this::convertStringToSubscription)
        .collect(Collectors.toList());
  }

  private static String convertSubscriptionToString(Subscription sub) {
    if (sub.getName().isEmpty() || sub.getProject().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Missing arguments in subscription string: %s", sub.toString()));
    }
    return String.format("%s:%s", sub.getProject(), sub.getName());
  }

  private Subscription convertStringToSubscription(String sub) {
    if (sub.equals("")) {
      return Subscription.newBuilder().build();
    }
    String[] split = sub.split(":", 2);
    return Subscription.newBuilder().setProject(split[0]).setName(split[1]).build();
  }
}
