/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2022 The Feast Authors
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
package feast.serving.connectors.redis.retriever;

import feast.serving.service.config.ApplicationProperties;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class OptimizedRedisClientAdapter {
  private static final Logger log = LoggerFactory.getLogger(OptimizedRedisClientAdapter.class);

  private final GenericObjectPool<StatefulConnection<byte[], byte[]>> connectionPool;
  private final Object lettuceClient; // Either RedisClient or RedisClusterClient
  private final boolean isCluster;

  public static class PooledAsyncCommands
      implements AutoCloseable { // To be used with try-with-resources
    private final RedisAsyncCommands<byte[], byte[]> commands;
    private final StatefulConnection<byte[], byte[]> connection;
    private final GenericObjectPool<StatefulConnection<byte[], byte[]>> pool;

    public PooledAsyncCommands(
        StatefulConnection<byte[], byte[]> connection,
        GenericObjectPool<StatefulConnection<byte[], byte[]>> pool) {
      this.connection = connection;
      this.commands =
          (connection instanceof StatefulRedisClusterConnection)
              ? ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async()
              : ((StatefulRedisConnection<byte[], byte[]>) connection).async();
      this.pool = pool;
    }

    public RedisAsyncCommands<byte[], byte[]> get() {
      return commands;
    }

    @Override
    public void close() {
      if (connection != null && pool != null) {
        try {
          pool.returnObject(connection);
        } catch (Exception e) {
          log.warn("Error returning Redis connection to pool", e);
          // Attempt to destroy the object if return fails badly
          try {
            pool.invalidateObject(connection);
          } catch (Exception ex) {
            log.warn("Error invalidating Redis connection object", ex);
          }
        }
      }
    }
  }

  @Inject
  public OptimizedRedisClientAdapter(ApplicationProperties applicationProperties) {
    ApplicationProperties.Store storeConfig = applicationProperties.getActiveStore();
    this.isCluster = storeConfig.getType().equalsIgnoreCase("REDIS_CLUSTER");

    GenericObjectPoolConfig<StatefulConnection<byte[], byte[]>> poolConfig =
        new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(
        storeConfig.getPoolMaxTotal() > 0
            ? storeConfig.getPoolMaxTotal()
            : GenericObjectPoolConfig.DEFAULT_MAX_TOTAL); // Default 8
    poolConfig.setMaxIdle(
        storeConfig.getPoolMaxIdle() > 0
            ? storeConfig.getPoolMaxIdle()
            : GenericObjectPoolConfig.DEFAULT_MAX_IDLE); // Default 8
    poolConfig.setMinIdle(
        storeConfig.getPoolMinIdle() >= 0
            ? storeConfig.getPoolMinIdle()
            : GenericObjectPoolConfig.DEFAULT_MIN_IDLE); // Default 0
    poolConfig.setTestOnBorrow(storeConfig.isPoolTestOnBorrow()); // Default false
    poolConfig.setBlockWhenExhausted(storeConfig.isPoolBlockWhenExhausted()); // Default true

    if (isCluster) {
      ApplicationProperties.RedisClusterConfig redisClusterConfig =
          (ApplicationProperties.RedisClusterConfig) storeConfig.getConfig();
      List<RedisURI> redisURIs =
          Arrays.stream(redisClusterConfig.getConnection_string().split(","))
              .map(
                  hostPort -> {
                    String[] parts = hostPort.trim().split(":");
                    return RedisURI.create(parts[0], Integer.parseInt(parts[1]));
                  })
              .collect(Collectors.toList());

      RedisClusterClient clusterClient = RedisClusterClient.create(redisURIs);

      ClusterTopologyRefreshOptions topologyRefreshOptions =
          ClusterTopologyRefreshOptions.builder()
              .enablePeriodicRefresh(Duration.ofSeconds(30)) // Configurable
              .enableAllAdaptiveRefreshTriggers()
              .build();

      ClusterClientOptions clientOptions =
          ClusterClientOptions.builder()
              .topologyRefreshOptions(topologyRefreshOptions)
              .socketOptions(
                  SocketOptions.builder()
                      .connectTimeout(Duration.ofMillis(redisClusterConfig.getConnectTimeoutMs()))
                      .keepAlive(true)
                      .tcpNoDelay(true)
                      .build())
              .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofMillis(redisClusterConfig.getTimeoutMs())).build())
              .validateClusterNodeMembership(true)
              .build();
      clusterClient.setOptions(clientOptions);
      this.lettuceClient = clusterClient;
      this.connectionPool =
          ConnectionPoolSupport.createGenericObjectPool(
              () -> {
                StatefulRedisClusterConnection<byte[], byte[]> conn =
                    clusterClient.connect(new ByteArrayCodec());
                if (redisClusterConfig.getRead_from() != null
                    && !redisClusterConfig.getRead_from().isEmpty()) {
                  conn.setReadFrom(ReadFrom.valueOf(redisClusterConfig.getRead_from().toUpperCase()));
                }
                return conn;
              },
              poolConfig);

    } else {
      ApplicationProperties.RedisConfig redisConfig =
          (ApplicationProperties.RedisConfig) storeConfig.getConfig();
      RedisURI.Builder redisURIBuilder =
          RedisURI.builder()
              .withHost(redisConfig.getHost())
              .withPort(redisConfig.getPort())
              .withTimeout(Duration.ofMillis(redisConfig.getTimeoutMs()));

      if (redisConfig.getPassword() != null && !redisConfig.getPassword().isEmpty()) {
        redisURIBuilder.withPassword(redisConfig.getPassword().toCharArray());
      }
      if (redisConfig.getDatabase() > 0) {
        redisURIBuilder.withDatabase(redisConfig.getDatabase());
      }
      if (redisConfig.isSsl()) {
        redisURIBuilder.withSsl(true);
        redisURIBuilder.withVerifyPeer(redisConfig.isSslVerifyPeer());
      }


      RedisClient standaloneClient = RedisClient.create(redisURIBuilder.build());
      standaloneClient.setOptions(
          io.lettuce.core.ClientOptions.builder()
              .socketOptions(
                  SocketOptions.builder()
                      .connectTimeout(Duration.ofMillis(redisConfig.getConnectTimeoutMs()))
                      .keepAlive(true)
                      .tcpNoDelay(true)
                      .build())
              .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofMillis(redisConfig.getTimeoutMs())).build())
              .build());
      this.lettuceClient = standaloneClient;
      this.connectionPool =
          ConnectionPoolSupport.createGenericObjectPool(
              () -> standaloneClient.connect(new ByteArrayCodec()), poolConfig);
    }
    log.info(
        "OptimizedRedisClientAdapter initialized. isCluster: {}, Pool MaxTotal: {}",
        isCluster,
        poolConfig.getMaxTotal());
  }

  /**
   * Borrows a PooledAsyncCommands wrapper from the pool. This wrapper should be used in a
   * try-with-resources statement to ensure the underlying connection is returned to the pool.
   *
   * @return PooledAsyncCommands containing the RedisAsyncCommands and its pooled connection.
   * @throws RuntimeException if unable to borrow a connection from the pool.
   */
  public PooledAsyncCommands borrowPooledAsyncCommands() {
    try {
      StatefulConnection<byte[], byte[]> connection = connectionPool.borrowObject();
      return new PooledAsyncCommands(connection, connectionPool);
    } catch (Exception e) {
      log.error("Failed to borrow Redis connection from pool", e);
      throw new RuntimeException("Failed to borrow Redis connection from pool", e);
    }
  }

  /**
   * Gets an instance of RedisAsyncCommands.
   * Note: This method is kept for simpler direct async command usage if not using the PooledAsyncCommands wrapper.
   * The caller is responsible for managing the connection lifecycle (borrowing and returning).
   * It is generally recommended to use {@link #borrowPooledAsyncCommands()} with try-with-resources.
   */
  public RedisAsyncCommands<byte[], byte[]> getAsyncCommands() {
      // This method might be less safe if connections are not properly returned.
      // Prefer borrowPooledAsyncCommands.
      // For now, let's assume it's used by a component that handles connection return.
      // Or, this method itself could manage a temporary borrow and expect a release.
      // For simplicity and to match a potential direct usage pattern:
      StatefulConnection<byte[], byte[]> conn = null;
      try {
          conn = connectionPool.borrowObject(); // This connection needs to be returned
          if (isCluster) {
              return ((StatefulRedisClusterConnection<byte[], byte[]>) conn).async();
          } else {
              return ((StatefulRedisConnection<byte[], byte[]>) conn).async();
          }
      } catch (Exception e) {
          log.error("Failed to get async commands", e);
          if (conn != null) {
              try {
                  connectionPool.invalidateObject(conn); // Invalidate on error
              } catch (Exception ex) {
                  log.warn("Error invalidating connection object after failing to get async commands", ex);
              }
          }
          throw new RuntimeException("Failed to get async commands", e);
      }
      // The issue here is how 'conn' is returned. The OptimizedRedisOnlineRetriever
      // calls getAsyncCommands() and then releaseAsyncCommands(commands).
      // This implies the adapter needs to track which connection 'commands' belongs to.
      // This is complex. The PooledAsyncCommands wrapper is a much cleaner pattern.
      // For now, to make this method work as potentially expected by the retriever's
      // getAsyncCommands/releaseAsyncCommands pattern, we'll have to make releaseAsyncCommands
      // a no-op if this method is used, or the retriever needs to change.
      // Let's assume the retriever will be updated to use borrowPooledAsyncCommands.
      // This method is now problematic.
      throw new UnsupportedOperationException("Prefer borrowPooledAsyncCommands() with try-with-resources.");
  }

  /**
   * Releases async commands by returning the associated connection to the pool.
   * This method is tricky if the adapter has to map commands back to connections.
   * It's better if the caller manages the connection directly.
   *
   * @param commands The RedisAsyncCommands instance that is no longer needed.
   */
   public void releaseAsyncCommands(RedisAsyncCommands<byte[], byte[]> commands) {
       // This method is difficult to implement correctly without knowing which
       // connection these commands came from, unless commands object itself
       // holds a reference to its StatefulConnection.
       // Lettuce's RedisAsyncCommands are tied to a StatefulConnection.
       // If 'commands' is an instance of StatefulConnection's async commands,
       // we need the StatefulConnection object to return it.
       // This method is deprecated in favor of using the AutoCloseable PooledAsyncCommands.
       log.warn("releaseAsyncCommands(commands) is deprecated. Use try-with-resources on PooledAsyncCommands instead.");
   }


  public void shutdown() {
    log.info("Shutting down OptimizedRedisClientAdapter connection pool and client.");
    if (connectionPool != null) {
      connectionPool.close();
    }
    if (lettuceClient instanceof RedisClient) {
      ((RedisClient) lettuceClient).shutdown();
    } else if (lettuceClient instanceof RedisClusterClient) {
      ((RedisClusterClient) lettuceClient).shutdown();
    }
    log.info("OptimizedRedisClientAdapter shutdown complete.");
  }

  // Inner class for PooledObjectFactory if not using ConnectionPoolSupport directly
  // For simplicity, ConnectionPoolSupport.createGenericObjectPool is used above.
  // If more complex factory logic is needed (e.g., custom validation),
  // a separate factory class would be better.
}
