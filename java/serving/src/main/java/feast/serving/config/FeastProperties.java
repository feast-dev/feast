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
package feast.serving.config;

// Feast configuration properties that maps Feast configuration from default application.yml file to
// a Java object.
// https://www.baeldung.com/configuration-properties-in-spring-boot
// https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-typesafe-configuration-properties

import feast.common.auth.config.SecurityProperties;
import feast.common.auth.config.SecurityProperties.AuthenticationProperties;
import feast.common.auth.config.SecurityProperties.AuthorizationProperties;
import feast.common.auth.credentials.CoreAuthenticationProperties;
import feast.common.logging.config.LoggingProperties;
import feast.storage.connectors.bigtable.retriever.BigTableStoreConfig;
import feast.storage.connectors.cassandra.retriever.CassandraStoreConfig;
import feast.storage.connectors.redis.retriever.RedisClusterStoreConfig;
import feast.storage.connectors.redis.retriever.RedisStoreConfig;
import io.lettuce.core.ReadFrom;
import java.time.Duration;
import java.util.*;
import javax.annotation.PostConstruct;
import javax.validation.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.ValidHost;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

/** Feast Serving properties. */
@ComponentScan("feast.common.logging")
@ConfigurationProperties(prefix = "feast", ignoreInvalidFields = true)
public class FeastProperties {

  /**
   * Instantiates a new Feast Serving properties.
   *
   * @param buildProperties the build properties
   */
  @Autowired
  public FeastProperties(BuildProperties buildProperties) {
    setVersion(buildProperties.getVersion());
  }

  /** Instantiates a new Feast class. */
  public FeastProperties() {}

  /* Feast Serving build version */
  @NotBlank private String version = "unknown";

  /* Feast Core host to connect to. */
  @ValidHost @NotBlank private String coreHost;

  /* Feast Core port to connect to. */
  @Positive private int coreGrpcPort;

  private String registry;

  public String getRegistry() {
    return registry;
  }

  public void setRegistry(final String registry) {
    this.registry = registry;
  }

  private String transformationServiceEndpoint;

  public String getTransformationServiceEndpoint() {
    return transformationServiceEndpoint;
  }

  public void setTransformationServiceEndpoint(final String transformationServiceEndpoint) {
    this.transformationServiceEndpoint = transformationServiceEndpoint;
  }

  private CoreAuthenticationProperties coreAuthentication;

  public CoreAuthenticationProperties getCoreAuthentication() {
    return coreAuthentication;
  }

  public void setCoreAuthentication(CoreAuthenticationProperties coreAuthentication) {
    this.coreAuthentication = coreAuthentication;
  }

  @Positive private int coreCacheRefreshInterval;

  private SecurityProperties security;

  @Bean
  SecurityProperties securityProperties() {
    return this.getSecurity();
  }

  /**
   * Getter for SecurityProperties
   *
   * @return Returns the {@link SecurityProperties} object.
   */
  public SecurityProperties getSecurity() {
    return security;
  }

  /**
   * Setter for SecurityProperties
   *
   * @param security :input {@link SecurityProperties} object
   */
  public void setSecurity(SecurityProperties security) {
    this.security = security;
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

  /**
   * Set the name of the active store found in the "stores" configuration list
   *
   * @param activeStore String name to active store
   */
  public void setActiveStore(String activeStore) {
    this.activeStore = activeStore;
  }

  /** Name of the active store configuration (only one store can be active at a time). */
  @NotBlank private String activeStore;

  /**
   * Collection of store configurations. The active store is selected by the "activeStore" field.
   */
  private List<Store> stores = new ArrayList<>();

  /* Metric tracing properties. */
  private TracingProperties tracing;

  /* Feast Audit Logging properties */
  @NotNull private LoggingProperties logging;

  @Bean
  LoggingProperties loggingProperties() {
    return getLogging();
  }

  /**
   * Gets Serving store configuration as a list of {@link Store}.
   *
   * @return List of stores objects
   */
  public List<Store> getStores() {
    return stores;
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
   * Sets build version
   *
   * @param version the build version
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Gets Feast Core host.
   *
   * @return Feast Core host
   */
  public String getCoreHost() {
    return coreHost;
  }

  /**
   * Sets Feast Core host to connect to.
   *
   * @param coreHost Feast Core host
   */
  public void setCoreHost(String coreHost) {
    this.coreHost = coreHost;
  }

  /**
   * Gets Feast Core gRPC port.
   *
   * @return Feast Core gRPC port
   */
  public int getCoreGrpcPort() {
    return coreGrpcPort;
  }

  /**
   * Sets Feast Core gRPC port.
   *
   * @param coreGrpcPort gRPC port of Feast Core
   */
  public void setCoreGrpcPort(int coreGrpcPort) {
    this.coreGrpcPort = coreGrpcPort;
  }

  /**
   * Gets CachedSpecService refresh interval.
   *
   * @return CachedSpecService refresh interval
   */
  public int getCoreCacheRefreshInterval() {
    return coreCacheRefreshInterval;
  }

  /**
   * Sets CachedSpecService refresh interval.
   *
   * @param coreCacheRefreshInterval CachedSpecService refresh interval
   */
  public void setCoreCacheRefreshInterval(int coreCacheRefreshInterval) {
    this.coreCacheRefreshInterval = coreCacheRefreshInterval;
  }

  /**
   * Sets the collection of configured stores.
   *
   * @param stores List of {@link Store}
   */
  public void setStores(List<Store> stores) {
    this.stores = stores;
  }

  /** Store configuration class for database that this Feast Serving uses. */
  public static class Store {

    private String name;

    private String type;

    private Map<String, String> config = new HashMap<>();

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

    /**
     * Sets the store type
     *
     * @param type the type
     */
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
    public Map<String, String> getConfig() {
      return config;
    }

    public RedisClusterStoreConfig getRedisClusterConfig() {
      return new RedisClusterStoreConfig(
          this.config.get("connection_string"),
          ReadFrom.valueOf(this.config.get("read_from")),
          Duration.parse(this.config.get("timeout")));
    }

    public RedisStoreConfig getRedisConfig() {
      return new RedisStoreConfig(
          this.config.get("host"),
          Integer.valueOf(this.config.get("port")),
          Boolean.valueOf(this.config.getOrDefault("ssl", "false")),
          this.config.getOrDefault("password", ""));
    }

    public BigTableStoreConfig getBigtableConfig() {
      return new BigTableStoreConfig(
          this.config.get("project_id"),
          this.config.get("instance_id"),
          this.config.get("app_profile_id"));
    }

    public CassandraStoreConfig getCassandraConfig() {
      return new CassandraStoreConfig(
          this.config.get("connection_string"),
          this.config.get("data_center"),
          this.config.get("keyspace"));
    }

    /**
     * Sets the store config. Please protos/feast/core/Store.proto for the specific options for each
     * store.
     *
     * @param config the config map
     */
    public void setConfig(Map<String, String> config) {
      this.config = config;
    }
  }

  public enum StoreType {
    BIGTABLE,
    CASSANDRA,
    REDIS,
    REDIS_CLUSTER;
  }

  /**
   * Gets tracing properties
   *
   * @return tracing properties
   */
  public TracingProperties getTracing() {
    return tracing;
  }

  /**
   * Sets the tracing configuration.
   *
   * @param tracing the tracing
   */
  public void setTracing(TracingProperties tracing) {
    this.tracing = tracing;
  }

  /**
   * Gets logging properties
   *
   * @return logging properties
   */
  public LoggingProperties getLogging() {
    return logging;
  }

  /**
   * Sets logging properties
   *
   * @param logging the logging properties
   */
  public void setLogging(LoggingProperties logging) {
    this.logging = logging;
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

  /**
   * Validates all FeastProperties. This method runs after properties have been initialized and
   * individually and conditionally validates each class.
   */
  @PostConstruct
  public void validate() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    // Validate root fields in FeastProperties
    Set<ConstraintViolation<FeastProperties>> violations = validator.validate(this);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }

    // Validate CoreAuthenticationProperties
    Set<ConstraintViolation<CoreAuthenticationProperties>> coreAuthenticationPropsViolations =
        validator.validate(getCoreAuthentication());
    if (!coreAuthenticationPropsViolations.isEmpty()) {
      throw new ConstraintViolationException(coreAuthenticationPropsViolations);
    }

    // Validate AuthenticationProperties
    Set<ConstraintViolation<AuthenticationProperties>> authenticationPropsViolations =
        validator.validate(getSecurity().getAuthentication());
    if (!authenticationPropsViolations.isEmpty()) {
      throw new ConstraintViolationException(authenticationPropsViolations);
    }

    // Validate AuthorizationProperties
    Set<ConstraintViolation<AuthorizationProperties>> authorizationPropsViolations =
        validator.validate(getSecurity().getAuthorization());
    if (!authorizationPropsViolations.isEmpty()) {
      throw new ConstraintViolationException(authorizationPropsViolations);
    }
  }
}
