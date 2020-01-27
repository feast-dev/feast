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
package feast.serving;

// Feast configuration properties that maps Feast configuration from default application.yml file to
// a Java object.
// https://www.baeldung.com/configuration-properties-in-spring-boot
// https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-typesafe-configuration-properties

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "feast")
public class FeastProperties {
  private String version;
  private String coreHost;
  private int coreGrpcPort;
  private StoreProperties store;
  private JobProperties jobs;
  private TracingProperties tracing;

  public String getVersion() {
    return this.version;
  }

  public String getCoreHost() {
    return this.coreHost;
  }

  public int getCoreGrpcPort() {
    return this.coreGrpcPort;
  }

  public StoreProperties getStore() {
    return this.store;
  }

  public JobProperties getJobs() {
    return this.jobs;
  }

  public TracingProperties getTracing() {
    return this.tracing;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public void setCoreHost(String coreHost) {
    this.coreHost = coreHost;
  }

  public void setCoreGrpcPort(int coreGrpcPort) {
    this.coreGrpcPort = coreGrpcPort;
  }

  public void setStore(StoreProperties store) {
    this.store = store;
  }

  public void setJobs(JobProperties jobs) {
    this.jobs = jobs;
  }

  public void setTracing(TracingProperties tracing) {
    this.tracing = tracing;
  }

  public static class StoreProperties {
    private String configPath;
    private int redisPoolMaxSize;
    private int redisPoolMaxIdle;
    private int cassandraPoolCoreLocalConnections;
    private int cassandraPoolMaxLocalConnections;
    private int cassandraPoolCoreRemoteConnections;
    private int cassandraPoolMaxRemoteConnections;
    private int cassandraPoolMaxRequestsLocalConnection;
    private int cassandraPoolMaxRequestsRemoteConnection;
    private int cassandraPoolNewLocalConnectionThreshold;
    private int cassandraPoolNewRemoteConnectionThreshold;
    private int cassandraPoolTimeoutMillis;

    public String getConfigPath() {
      return this.configPath;
    }

    public int getRedisPoolMaxSize() {
      return this.redisPoolMaxSize;
    }

    public int getRedisPoolMaxIdle() {
      return this.redisPoolMaxIdle;
    }

    public int getCassandraPoolCoreLocalConnections() {
      return this.cassandraPoolCoreLocalConnections;
    }

    public int getCassandraPoolMaxLocalConnections() {
      return this.cassandraPoolMaxLocalConnections;
    }

    public int getCassandraPoolCoreRemoteConnections() {
      return this.cassandraPoolCoreRemoteConnections;
    }

    public int getCassandraPoolMaxRemoteConnections() {
      return this.cassandraPoolMaxRemoteConnections;
    }

    public int getCassandraPoolMaxRequestsLocalConnection() {
      return this.cassandraPoolMaxRequestsLocalConnection;
    }

    public int getCassandraPoolMaxRequestsRemoteConnection() {
      return this.cassandraPoolMaxRequestsRemoteConnection;
    }

    public int getCassandraPoolNewLocalConnectionThreshold() {
      return this.cassandraPoolNewLocalConnectionThreshold;
    }

    public int getCassandraPoolNewRemoteConnectionThreshold() {
      return this.cassandraPoolNewRemoteConnectionThreshold;
    }

    public int getCassandraPoolTimeoutMillis() {
      return this.cassandraPoolTimeoutMillis;
    }

    public void setConfigPath(String configPath) {
      this.configPath = configPath;
    }

    public void setRedisPoolMaxSize(int redisPoolMaxSize) {
      this.redisPoolMaxSize = redisPoolMaxSize;
    }

    public void setRedisPoolMaxIdle(int redisPoolMaxIdle) {
      this.redisPoolMaxIdle = redisPoolMaxIdle;
    }

    public void setCassandraPoolCoreLocalConnections(int cassandraPoolCoreLocalConnections) {
      this.cassandraPoolCoreLocalConnections = cassandraPoolCoreLocalConnections;
    }

    public void setCassandraPoolMaxLocalConnections(int cassandraPoolMaxLocalConnections) {
      this.cassandraPoolMaxLocalConnections = cassandraPoolMaxLocalConnections;
    }

    public void setCassandraPoolCoreRemoteConnections(int cassandraPoolCoreRemoteConnections) {
      this.cassandraPoolCoreRemoteConnections = cassandraPoolCoreRemoteConnections;
    }

    public void setCassandraPoolMaxRemoteConnections(int cassandraPoolMaxRemoteConnections) {
      this.cassandraPoolMaxRemoteConnections = cassandraPoolMaxRemoteConnections;
    }

    public void setCassandraPoolMaxRequestsLocalConnection(
        int cassandraPoolMaxRequestsLocalConnection) {
      this.cassandraPoolMaxRequestsLocalConnection = cassandraPoolMaxRequestsLocalConnection;
    }

    public void setCassandraPoolMaxRequestsRemoteConnection(
        int cassandraPoolMaxRequestsRemoteConnection) {
      this.cassandraPoolMaxRequestsRemoteConnection = cassandraPoolMaxRequestsRemoteConnection;
    }

    public void setCassandraPoolNewLocalConnectionThreshold(
        int cassandraPoolNewLocalConnectionThreshold) {
      this.cassandraPoolNewLocalConnectionThreshold = cassandraPoolNewLocalConnectionThreshold;
    }

    public void setCassandraPoolNewRemoteConnectionThreshold(
        int cassandraPoolNewRemoteConnectionThreshold) {
      this.cassandraPoolNewRemoteConnectionThreshold = cassandraPoolNewRemoteConnectionThreshold;
    }

    public void setCassandraPoolTimeoutMillis(int cassandraPoolTimeoutMillis) {
      this.cassandraPoolTimeoutMillis = cassandraPoolTimeoutMillis;
    }
  }

  public static class JobProperties {
    private String stagingLocation;
    private String stagingProject;
    private int bigqueryInitialRetryDelaySecs;
    private int bigqueryTotalTimeoutSecs;
    private String storeType;
    private Map<String, String> storeOptions;

    public String getStagingLocation() {
      return this.stagingLocation;
    }

    public String getStagingProject() {
      return this.stagingProject;
    }

    public int getBigqueryInitialRetryDelaySecs() {
      return bigqueryInitialRetryDelaySecs;
    }

    public int getBigqueryTotalTimeoutSecs() {
      return bigqueryTotalTimeoutSecs;
    }

    public String getStoreType() {
      return this.storeType;
    }

    public Map<String, String> getStoreOptions() {
      return this.storeOptions;
    }

    public void setStagingLocation(String stagingLocation) {
      this.stagingLocation = stagingLocation;
    }

    public void setStagingProject(String stagingProject) {
      this.stagingProject = stagingProject;
    }

    public void setBigqueryInitialRetryDelaySecs(int bigqueryInitialRetryDelaySecs) {
      this.bigqueryInitialRetryDelaySecs = bigqueryInitialRetryDelaySecs;
    }

    public void setBigqueryTotalTimeoutSecs(int bigqueryTotalTimeoutSecs) {
      this.bigqueryTotalTimeoutSecs = bigqueryTotalTimeoutSecs;
    }

    public void setStoreType(String storeType) {
      this.storeType = storeType;
    }

    public void setStoreOptions(Map<String, String> storeOptions) {
      this.storeOptions = storeOptions;
    }
  }

  public static class TracingProperties {
    private boolean enabled;
    private String tracerName;
    private String serviceName;

    public boolean isEnabled() {
      return this.enabled;
    }

    public String getTracerName() {
      return this.tracerName;
    }

    public String getServiceName() {
      return this.serviceName;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public void setTracerName(String tracerName) {
      this.tracerName = tracerName;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }
  }
}
