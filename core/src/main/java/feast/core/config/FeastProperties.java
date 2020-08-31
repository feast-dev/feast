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

import feast.common.auth.config.SecurityProperties;
import feast.common.auth.config.SecurityProperties.AuthenticationProperties;
import feast.common.auth.config.SecurityProperties.AuthorizationProperties;
import feast.common.logging.config.LoggingProperties;
import feast.common.validators.OneOfStrings;
import feast.core.config.FeastProperties.StreamProperties.FeatureStreamOptions;
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
