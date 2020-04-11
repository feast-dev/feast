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
   * The "store" string should contain a YAML representation of the store configuration. Store
   * configurations can be seen in protos/feast/core/Store.proto
   */
  private feast.core.model.Store store;

  /* Job Store properties to retain state of async jobs. */
  private JobStoreProperties jobStore;

  /* Metric tracing properties. */
  private TracingProperties tracing;

  /**
   * Gets Serving store configuration deserialiazed as a {@link feast.core.model.Store}.
   *
   * @return the store
   */
  public feast.core.model.Store getStore() {
    return store;
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
   * Sets store properties.
   *
   * @param store properties comes from a YAML string
   */
  public void setStore(feast.core.model.Store store) {
    this.store = store;
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
