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
package feast.core.job.dataflow;

import java.util.Map;
import java.util.Set;
import javax.validation.*;
import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.BeanUtils;

@Getter
@Setter
public class DataflowRunnerConfig {

  public DataflowRunnerConfig(Map<String, String> runnerConfigOptions) {
    BeanUtils.copyProperties(this, runnerConfigOptions);
    validate();
  }

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
   * (Dataflow Runner Only) A pipeline level default location for storing temporary files. Support
   * Google Cloud Storage locations, e.g. gs://bucket/object
   */
  @NotBlank private String tempLocation;

  /* (Dataflow Runner Only) The maximum number of workers to use for the workerpool. */
  private Integer maxNumWorkers;

  /* BigQuery table specification, e.g. PROJECT_ID:DATASET_ID.PROJECT_ID */
  private String deadLetterTableSpec;

  public void validate() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    Set<ConstraintViolation<DataflowRunnerConfig>> dataflowRunnerConfigViolation =
        validator.validate(this);
    if (!dataflowRunnerConfigViolation.isEmpty()) {
      throw new ConstraintViolationException(dataflowRunnerConfigViolation);
    }
  }
}
