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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import feast.core.config.FeastProperties.JobProperties.RunnerOptions;
import feast.core.config.FeastProperties.StreamProperties.FeatureStreamOptions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.URL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.info.BuildProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "feast", ignoreInvalidFields = true)
public class FeastProperties {

  @Autowired
  public FeastProperties(BuildProperties buildProperties) {
    setVersion(buildProperties.getVersion());
  }

  public FeastProperties() {}

  /* Feast Core Build Version */
  @NotBlank private String version = "unknown";

  /* Population job properties */
  @NotNull private JobProperties jobs;

  @NotNull
  /* Feast Kafka stream properties */
  private StreamProperties stream;

  @Getter
  @Setter
  public static class JobProperties {

    @NotBlank
    /* Apache Beam runner type. Possible options: DirectRunner, DataflowRunner */
    private String runner;

    /* Apache Beam runner options for population jobs */
    private RunnerOptions runnerOptions;

    /* (Optional) Additional arguments to pass to Beam population jobs */
    private Map<String, String> extraRunnerOptions;

    @NotNull
    /* Population job metric properties */
    private MetricsProperties metrics;

    /* Timeout in seconds for each attempt to update or submit a new job to the runner */
    @Positive private long jobUpdateTimeout;

    /* Job update polling interval in millisecond. How frequently Feast will update running jobs. */
    @Positive private long pollingIntervalMillis;

    /** Apache Beam runner options for population jobs */
    @Getter
    @Setter
    public static class RunnerOptions {

      /* (Dataflow Runner Only) Project id to use when launching jobs. */
      @NotBlank private String project;

      /* (Dataflow Runner Only) The Google Compute Engine region for creating Dataflow jobs. */
      @NotBlank private String region;

      /* (Dataflow Runner Only) GCP availability zone for operations. */
      @NotBlank private String zone;

      /* (Dataflow Runner Only) Run the job as a specific service account, instead of the default GCE robot. */
      @NotBlank private String serviceAccount;

      /* (Dataflow Runner Only) GCE network for launching workers. */
      @NotBlank private String network;

      /* (Dataflow Runner Only) GCE subnetwork for launching workers. */
      @NotBlank private String subnetwork;

      /* (Dataflow Runner Only) Machine type to create Dataflow worker VMs as. */
      private String workerMachineType;

      /* (Dataflow Runner Only) The autoscaling algorithm to use for the workerpool. */
      private String autoscalingAlgorithm;

      /* (Dataflow Runner Only) Specifies whether worker pools should be started with public IP addresses. */
      private Boolean usePublicIps;

      /**
       * (Dataflow Runner Only) A pipeline level default location for storing temporary files.
       * Support Google Cloud Storage locations, e.g. gs://bucket/object
       */
      @NotBlank private String tempLocation;

      /* (Dataflow Runner Only) The maximum number of workers to use for the workerpool. */
      private Integer maxNumWorkers;

      /**
       * (Direct Runner Only) Controls the amount of target parallelism the DirectRunner will use.
       * Defaults to the greater of the number of available processors and 3. Must be a value
       * greater than zero.
       */
      private Integer targetParallelism;

      /* BigQuery table specification, e.g. PROJECT_ID:DATASET_ID.PROJECT_ID */
      private String deadLetterTableSpec;
    }

    public Map<String, String> getRunnerOptionsMap() {
      // First collect the existing "extra options"
      Map<String, String> combinedOptions = new HashMap<String, String>(getExtraRunnerOptions());

      // Convert all fields in RunnerOptions to <String, String> and merge
      ObjectMapper oMapper = new ObjectMapper();
      combinedOptions.putAll(
          oMapper.convertValue(
              getRunnerOptions(), new TypeReference<HashMap<String, String>>() {}));

      return combinedOptions;
    }
  }

  @AssertTrue
  public boolean isValidJobRunnerSelected() {
    String[] validRunners = new String[] {"DataflowRunner", "DirectRunner"};
    return Arrays.asList(validRunners).contains(getJobs().getRunner());
  }

  /** Properties used to configure Feast's managed Kafka feature stream. */
  @Getter
  @Setter
  public static class StreamProperties {

    /* Feature stream type. Only "kafka" is supported. */
    @NotBlank private String type;

    /* Feature stream options */
    @NotNull private FeatureStreamOptions options;

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
  }

  @AssertTrue
  public boolean isValidStreamTypeSelected() {
    return Objects.equals(getStream().getType(), "kafka");
  }

  /** Feast population job metrics */
  @Getter
  @Setter
  public static class MetricsProperties {

    /* Population job metrics enabled */
    private boolean enabled;

    /* Metric type. Possible options: statsd */
    @NotBlank private String type;

    /* Host of metric sink */
    @URL private String host;

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

    // Validate RunnerOptions
    Set<ConstraintViolation<RunnerOptions>> runnerOptionsViolations =
        validator.validate(getJobs().getRunnerOptions());
    if (!runnerOptionsViolations.isEmpty()) {
      throw new ConstraintViolationException(runnerOptionsViolations);
    }

    // Validate MetricsProperties
    if (getJobs().getMetrics().isEnabled()) {
      Set<ConstraintViolation<MetricsProperties>> jobMetricViolations =
          validator.validate(getJobs().getMetrics());
      if (!jobMetricViolations.isEmpty()) {
        throw new ConstraintViolationException(jobMetricViolations);
      }
    }
  }
}
