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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import feast.core.StoreProto;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.ValidHost;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.info.BuildProperties;

/** Feast Serving properties. */
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

  public FeastProperties() {}

  /* Feast Serving build version */
  @NotBlank private String version = "unknown";

  /* Feast Core host to connect to. */
  @ValidHost @NotBlank private String coreHost;

  /* Feast Core port to connect to. */
  @Positive private int coreGrpcPort;

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

  /* Job Store properties to retain state of async jobs. */
  private JobStoreProperties jobStore;

  /* Metric tracing properties. */
  private TracingProperties tracing;

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
   * @return Port
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
   * Sets the collection of configured stores.
   *
   * @param stores List of {@link Store}
   */
  public void setStores(List<Store> stores) {
    this.stores = stores;
  }

  public static class Store {

    private String name;

    private String type;

    private Map<String, String> config = new HashMap<>();

    private List<Subscription> subscriptions = new ArrayList<>();

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public StoreProto.Store toProto()
        throws InvalidProtocolBufferException, JsonProcessingException {
      List<Subscription> subscriptions = getSubscriptions();
      List<StoreProto.Store.Subscription> subscriptionProtos =
          subscriptions.stream().map(Subscription::toProto).collect(Collectors.toList());

      StoreProto.Store.Builder storeProtoBuilder =
          StoreProto.Store.newBuilder()
              .setName(name)
              .setType(StoreProto.Store.StoreType.valueOf(type))
              .addAllSubscriptions(subscriptionProtos);

      ObjectMapper jsonWriter = new ObjectMapper();

      // TODO: All of this logic should be moved to the store layer. Only a Map<String, String>
      // should be sent to a store and it should do its own validation.
      switch (StoreProto.Store.StoreType.valueOf(type)) {
        case REDIS:
          StoreProto.Store.RedisConfig.Builder redisConfig =
              StoreProto.Store.RedisConfig.newBuilder();
          JsonFormat.parser().merge(jsonWriter.writeValueAsString(config), redisConfig);
          return storeProtoBuilder.setRedisConfig(redisConfig.build()).build();
        case BIGQUERY:
          StoreProto.Store.BigQueryConfig.Builder bqConfig =
              StoreProto.Store.BigQueryConfig.newBuilder();
          JsonFormat.parser().merge(jsonWriter.writeValueAsString(config), bqConfig);
          return storeProtoBuilder.setBigqueryConfig(bqConfig.build()).build();
        case CASSANDRA:
          StoreProto.Store.CassandraConfig.Builder cassandraConfig =
              StoreProto.Store.CassandraConfig.newBuilder();
          JsonFormat.parser().merge(jsonWriter.writeValueAsString(config), cassandraConfig);
          return storeProtoBuilder.setCassandraConfig(cassandraConfig.build()).build();
        default:
          throw new InvalidProtocolBufferException("Invalid store set");
      }
    }

    private List<Subscription> getSubscriptions() {
      return subscriptions;
    }

    public Map<String, String> getConfig() {
      return config;
    }

    public void setConfig(Map<String, String> config) {
      this.config = config;
    }

    public void setSubscriptions(List<Subscription> subscriptions) {
      this.subscriptions = subscriptions;
    }

    public class Subscription {
      String project;
      String name;
      String version;

      public String getProject() {
        return project;
      }

      public void setProject(String project) {
        this.project = project;
      }

      public String getName() {
        return name;
      }

      public void setName(String name) {
        this.name = name;
      }

      public String getVersion() {
        return version;
      }

      public void setVersion(String version) {
        this.version = version;
      }

      public StoreProto.Store.Subscription toProto() {
        return StoreProto.Store.Subscription.newBuilder()
            .setName(getName())
            .setProject(getProject())
            .setVersion(getVersion())
            .build();
      }
    }
  }

  /**
   * Gets job store properties
   *
   * @return the job store properties
   */
  public JobStoreProperties getJobStore() {
    return jobStore;
  }

  /**
   * Set job store properties
   *
   * @param jobStore Job store properties to set
   */
  public void setJobStore(JobStoreProperties jobStore) {
    this.jobStore = jobStore;
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

  /** The type Job store properties. */
  public static class JobStoreProperties {

    /** Job Store Redis Host */
    private String redisHost;

    /** Job Store Redis Host */
    private int redisPort;

    /**
     * Gets redis host.
     *
     * @return the redis host
     */
    public String getRedisHost() {
      return redisHost;
    }

    /**
     * Sets redis host.
     *
     * @param redisHost the redis host
     */
    public void setRedisHost(String redisHost) {
      this.redisHost = redisHost;
    }

    /**
     * Gets redis port.
     *
     * @return the redis port
     */
    public int getRedisPort() {
      return redisPort;
    }

    /**
     * Sets redis port.
     *
     * @param redisPort the redis port
     */
    public void setRedisPort(int redisPort) {
      this.redisPort = redisPort;
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
