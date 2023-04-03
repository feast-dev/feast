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
package feast.serving.service.config;

// Feast configuration properties that maps Feast configuration from default application.yml file to
// a Java object.
// https://www.baeldung.com/configuration-properties-in-spring-boot
// https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-typesafe-configuration-properties

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.OptBoolean;
import feast.serving.connectors.redis.retriever.RedisClusterStoreConfig;
import feast.serving.connectors.redis.retriever.RedisStoreConfig;
import io.lettuce.core.ReadFrom;
import java.time.Duration;
import java.util.*;
import javax.annotation.PostConstruct;
import javax.validation.*;
import javax.validation.constraints.NotBlank;
import org.slf4j.Logger;

/** Feast Serving properties. */
public class ApplicationProperties {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(ApplicationProperties.class);
  private FeastProperties feast;
  private GrpcServer grpc;

  public FeastProperties getFeast() {
    return feast;
  }

  public void setFeast(FeastProperties feast) {
    this.feast = feast;
  }

  public GrpcServer getGrpc() {
    return grpc;
  }

  public void setGrpc(GrpcServer grpc) {
    this.grpc = grpc;
  }

  /**
   * Validates all FeastProperties. This method runs after properties have been initialized and
   * individually and conditionally validates each class.
   */
  @PostConstruct
  public void validate() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    // Validate root fields in FeastProperties
    Set<ConstraintViolation<ApplicationProperties>> violations = validator.validate(this);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }
  }

  public enum StoreType {
    REDIS,
    REDIS_CLUSTER;
  }

  public static class FeastProperties {
    /* Feast Serving build version */
    @NotBlank private String version = "unknown";
    @NotBlank private String registry;
    @NotBlank private String project;
    private int registryRefreshInterval;
    private int entityKeySerializationVersion;
    /** Name of the active store configuration (only one store can be active at a time). */
    @NotBlank private String activeStore;
    /**
     * Collection of store configurations. The active store is selected by the "activeStore" field.
     */
    @JsonMerge(OptBoolean.FALSE)
    private List<Store> stores = new ArrayList<>();
    /* Metric tracing properties. */
    private TracingProperties tracing;
    private String gcpProject;
    private String awsRegion;
    private String transformationServiceEndpoint;

    public String getRegistry() {
      return registry;
    }

    public void setRegistry(String registry) {
      this.registry = registry;
    }

    public String getProject() {
      return project;
    }

    public void setProject(final String project) {
      this.project = project;
    }

    public int getRegistryRefreshInterval() {
      return registryRefreshInterval;
    }

    public void setRegistryRefreshInterval(int registryRefreshInterval) {
      this.registryRefreshInterval = registryRefreshInterval;
    }

    public int getEntityKeySerializationVersion() {
      return entityKeySerializationVersion;
    }

    public void setEntityKeySerializationVersion(int entityKeySerializationVersion) {
      this.entityKeySerializationVersion = entityKeySerializationVersion;
    }

    /**
     * Finds and returns the active store
     *
     * @return Returns the {@link Store} model object
     */
    public Store getActiveStore() {
      for (Store store : getStores()) {
        if (activeStore.equals(store.getName())) {
          return store;
        }
      }
      throw new RuntimeException(
          String.format("Active store is misconfigured. Could not find store: %s.", activeStore));
    }

    public void setActiveStore(String activeStore) {
      this.activeStore = activeStore;
    }

    /**
     * Gets Serving store configuration as a list of {@link Store}.
     *
     * @return List of stores objects
     */
    public List<Store> getStores() {
      return stores;
    }

    public void setStores(List<Store> stores) {
      this.stores = stores;
    }

    /**
     * Gets Feast Serving build version.
     *
     * @return the build version
     */
    public String getVersion() {
      return version;
    }

    /**
     * Gets tracing properties
     *
     * @return tracing properties
     */
    public TracingProperties getTracing() {
      return tracing;
    }

    public void setTracing(TracingProperties tracing) {
      this.tracing = tracing;
    }

    public String getGcpProject() {
      return gcpProject;
    }

    public void setGcpProject(String gcpProject) {
      this.gcpProject = gcpProject;
    }

    public String getAwsRegion() {
      return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
      this.awsRegion = awsRegion;
    }

    public String getTransformationServiceEndpoint() {
      return transformationServiceEndpoint;
    }

    public void setTransformationServiceEndpoint(String transformationServiceEndpoint) {
      this.transformationServiceEndpoint = transformationServiceEndpoint;
    }
  }

  /** Store configuration class for database that this Feast Serving uses. */
  public static class Store {

    private String name;

    private String type;

    private Map<String, String> config = new HashMap<>();

    // default construct for deserialization
    public Store() {}

    public Store(String name, String type, Map<String, String> config) {
      this.name = name;
      this.type = type;
      this.config = config;
    }

    /**
     * Gets name of this store. This is unique to this specific instance.
     *
     * @return the name of the store
     */
    public String getName() {
      return name;
    }

    /**
     * Sets the name of this store.
     *
     * @param name the name of the store
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets the store type. Example are REDIS, REDIS_CLUSTER, BIGTABLE or CASSANDRA
     *
     * @return the store type as a String.
     */
    public StoreType getType() {
      return StoreType.valueOf(this.type);
    }

    public void setType(String type) {
      this.type = type;
    }

    /**
     * Gets the configuration to this specific store. This is a map of strings. These options are
     * unique to the store. Please see protos/feast/core/Store.proto for the store specific
     * configuration options
     *
     * @return Returns the store specific configuration
     */
    public RedisClusterStoreConfig getRedisClusterConfig() {
      String read_from;
      if (!this.config.containsKey("read_from") || this.config.get("read_from") == null) {
        log.info("'read_from' not defined in Redis cluster config, so setting to UPSTREAM");
        read_from = ReadFrom.UPSTREAM.toString();
      } else {
        read_from = this.config.get("read_from");
      }

      if (!this.config.containsKey("timeout") || this.config.get("timeout") == null) {
        throw new IllegalArgumentException(
            "Redis cluster config does not have 'timeout' specified");
      }

      Boolean ssl = null;
      if (!this.config.containsKey("ssl") || this.config.get("ssl") == null) {
        log.info("'ssl' not defined in Redis cluster config, so setting to false");
        ssl = false;
      } else {
        ssl = Boolean.parseBoolean(this.config.get("ssl"));
      }
      Duration timeout = Duration.parse(this.config.get("timeout"));
      return new RedisClusterStoreConfig(
          this.config.get("connection_string"),
          ReadFrom.valueOf(read_from),
          timeout,
          ssl,
          this.config.getOrDefault("password", ""));
    }

    public RedisStoreConfig getRedisConfig() {
      return new RedisStoreConfig(
          this.config.get("host"),
          Integer.valueOf(this.config.get("port")),
          Boolean.valueOf(this.config.getOrDefault("ssl", "false")),
          this.config.getOrDefault("password", ""));
    }

    public void setConfig(Map<String, String> config) {
      this.config = config;
    }
  }

  public static class Server {
    private int port;

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }
  }

  public static class GrpcServer {
    private Server server;

    public Server getServer() {
      return server;
    }

    public void setServer(Server server) {
      this.server = server;
    }
  }

  /** Trace metric collection properties */
  public static class TracingProperties {

    /** Tracing enabled/disabled */
    private boolean enabled;

    /** Name of tracer to use (only "jaeger") */
    private String tracerName;

    /** Service name uniquely identifies this Feast Serving deployment */
    private String serviceName;

    /**
     * Is tracing enabled
     *
     * @return boolean flag
     */
    public boolean isEnabled() {
      return enabled;
    }

    /**
     * Sets tracing enabled or disabled.
     *
     * @param enabled flag
     */
    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    /**
     * Gets tracer name ('jaeger')
     *
     * @return the tracer name
     */
    public String getTracerName() {
      return tracerName;
    }

    /**
     * Sets tracer name.
     *
     * @param tracerName the tracer name
     */
    public void setTracerName(String tracerName) {
      this.tracerName = tracerName;
    }

    /**
     * Gets the service name. The service name uniquely identifies this Feast serving instance.
     *
     * @return the service name
     */
    public String getServiceName() {
      return serviceName;
    }

    /**
     * Sets service name.
     *
     * @param serviceName the service name
     */
    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }
  }
}
