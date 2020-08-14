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
package feast.core.config;

import feast.auth.config.SecurityProperties;
import feast.auth.config.SecurityProperties.AuthenticationProperties;
import feast.auth.config.SecurityProperties.AuthorizationProperties;
import feast.common.logging.config.LoggingProperties;
import feast.common.validators.OneOfStrings;
import feast.core.config.FeastProperties.StreamProperties.FeatureStreamOptions;
import feast.proto.core.StoreProto;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ComponentScan("feast.common.logging")
@ConfigurationProperties(prefix = "feast", ignoreInvalidFields = true)
public class FeastProperties {

  /**
   * Instantiates a new Feast properties.
   *
   * @param buildProperties Feast build properties
   */
  @Autowired
  public FeastProperties(BuildProperties buildProperties) {
    setVersion(buildProperties.getVersion());
  }

  /** Instantiates a new Feast properties. */
  public FeastProperties() {}

  /* Feast Core Build Version */
  @NotBlank private String version = "unknown";

  /* Population job properties */
  @NotNull private JobProperties jobs;

  @NotNull
  /* Feast Kafka stream properties */
  private StreamProperties stream;

  @NotNull private SecurityProperties security;

  @Bean
  SecurityProperties securityProperties() {
    return getSecurity();
  }

  /* Feast Audit Logging properties */
  @NotNull private LoggingProperties logging;

  @Bean
  LoggingProperties loggingProperties() {
    return getLogging();
  }

  /** Feast job properties. These properties are used for ingestion jobs. */
  @Getter
  @Setter
  public static class JobProperties {
    /* Toggle for enabling/disabling job management */
    private Boolean enabled = true;

    @NotBlank
    /* The active Apache Beam runner name. This name references one instance of the Runner class */
    private String activeRunner;

    /* Job Coordinator related properties */
    private CoordinatorProperties coordinator;

    @Getter
    @Setter
    public static class CoordinatorProperties {
      /* If true only one IngestionJob would be created per source with all subscribed stores in it */
      private Boolean consolidateJobsPerSource = false;

      /* Labels to identify jobs managed by this job coordinator */
      private Map<String, String> jobSelector = new HashMap<>();

      /* Selectors to define featureSets that are responsibility of current JobManager */
      private List<FeatureSetSelector> featureSetSelector = new ArrayList<>();

      /* Specify names of stores that must be used by current JobManager */
      private List<String> whitelistedStores = new ArrayList<>();

      /**
       * Similarly to Store's subscription this selector defines set of FeatureSets. All FeatureSets
       * that match both project and name will be selected. Project and name may use *
       */
      @Getter
      @Setter
      public static class FeatureSetSelector {
        private String project;
        private String name;

        public StoreProto.Store.Subscription toSubscription() {
          return StoreProto.Store.Subscription.newBuilder()
              .setName(this.name)
              .setProject(this.project)
              .build();
        }
      }
    }

    /** List of configured job runners. */
    private List<Runner> runners = new ArrayList<>();

    /**
     * Gets a {@link Runner} instance of the active runner
     *
     * @return the active runner
     */
    public Runner getActiveRunner() {
      for (Runner runner : getRunners()) {
        if (activeRunner.equals(runner.getName())) {
          return runner;
        }
      }
      throw new RuntimeException(
          String.format(
              "Active runner is misconfigured. Could not find runner: %s.", activeRunner));
    }

    /** Job Runner class. */
    @Getter
    @Setter
    public static class Runner {

      /** Job runner name. This must be unique. */
      String name;

      /** Job runner type DirectRunner, DataflowRunner currently supported */
      String type;

      /**
       * Job runner configuration options. See the following for options
       * https://api.docs.feast.dev/grpc/feast.core.pb.html#Runner
       */
      Map<String, Object> options = new HashMap<>();

      /**
       * Gets the job runner type as an enum.
       *
       * @return Returns the job runner type as {@link feast.core.job.Runner}
       */
      public feast.core.job.Runner getType() {
        return feast.core.job.Runner.fromString(type);
      }
    }

    @NotNull
    /* Population job metric properties */
    private MetricsProperties metrics;

    /* Timeout in seconds for each attempt to update or submit a new job to the runner */
    @Positive private long jobUpdateTimeoutSeconds;

    /* Job update polling interval in millisecond. How frequently Feast will update running jobs. */
    @Positive private long pollingIntervalMilliseconds;
  }

  /** Properties used to configure Feast's managed Kafka feature stream. */
  @Getter
  @Setter
  public static class StreamProperties {

    /* Feature stream type. Only "kafka" is supported. */
    @OneOfStrings({"kafka"})
    @NotBlank
    private String type;

    /* Feature stream options */
    @NotNull private FeatureStreamOptions options;

    /* FeatureSetSpec stream options - communication channel between SpecService and IngestionJob
     * to update Spec inside job w/o restart */
    @NotNull private FeatureSetSpecStreamProperties specsOptions;

    /** Feature stream options */
    @Getter
    @Setter
    public static class FeatureStreamOptions {

      /* Kafka topic to use for feature sets without source topics. */
      @NotBlank private String topic = "feast-features";

      /**
       * Comma separated list of Kafka bootstrap servers. Used for feature sets without a defined
       * source.
       */
      @NotBlank private String bootstrapServers = "localhost:9092";

      /* Defines the number of copies of managed feature stream Kafka. */
      @Positive private short replicationFactor = 1;

      /* Number of Kafka partitions to to use for managed feature stream. */
      @Positive private int partitions = 1;
    }

    @Getter
    @Setter
    public static class FeatureSetSpecStreamProperties {
      /* Kafka topic to send feature set spec to ingestion streaming job */
      @NotBlank private String specsTopic = "feast-feature-set-specs";

      /* Kafka topic to receive acknowledgment from ingestion job on successful processing of new specs */
      @NotBlank private String specsAckTopic = "feast-feature-set-specs-ack";

      /* Notify jobs interval in millisecond.
      How frequently Feast will check on Pending FeatureSets and publish them to kafka. */
      @Positive private long notifyIntervalMilliseconds;
    }
  }

  /** Feast population job metrics */
  @Getter
  @Setter
  public static class MetricsProperties {

    /* Population job metrics enabled */
    private boolean enabled;

    /* Metric type. Possible options: statsd */
    @OneOfStrings({"statsd"})
    @NotBlank
    private String type;

    /* Host of metric sink */
    private String host;

    /* Port of metric sink */
    @Positive private int port;
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

    // Validate Stream properties
    Set<ConstraintViolation<StreamProperties>> streamPropertyViolations =
        validator.validate(getStream());
    if (!streamPropertyViolations.isEmpty()) {
      throw new ConstraintViolationException(streamPropertyViolations);
    }

    // Validate Stream Options
    Set<ConstraintViolation<FeatureStreamOptions>> featureStreamOptionsViolations =
        validator.validate(getStream().getOptions());
    if (!featureStreamOptionsViolations.isEmpty()) {
      throw new ConstraintViolationException(featureStreamOptionsViolations);
    }

    // Validate JobProperties
    Set<ConstraintViolation<JobProperties>> jobPropertiesViolations = validator.validate(getJobs());
    if (!jobPropertiesViolations.isEmpty()) {
      throw new ConstraintViolationException(jobPropertiesViolations);
    }

    // Validate MetricsProperties
    if (getJobs().getMetrics().isEnabled()) {
      Set<ConstraintViolation<MetricsProperties>> jobMetricViolations =
          validator.validate(getJobs().getMetrics());
      if (!jobMetricViolations.isEmpty()) {
        throw new ConstraintViolationException(jobMetricViolations);
      }
      // Additional custom check for hostname value because there is no built-in Spring annotation
      // to validate the value is a DNS resolvable hostname or an IP address.
      try {
        //noinspection ResultOfMethodCallIgnored
        InetAddress.getByName(getJobs().getMetrics().getHost());
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException(
            "Invalid config value for feast.jobs.metrics.host: "
                + getJobs().getMetrics().getHost()
                + ". Make sure it is a valid IP address or DNS hostname e.g. localhost or 10.128.10.40. Error detail: "
                + e.getMessage());
      }
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
