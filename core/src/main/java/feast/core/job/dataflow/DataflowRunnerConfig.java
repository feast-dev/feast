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

import feast.core.job.option.RunnerConfig;
import feast.proto.core.RunnerProto.DataflowRunnerConfigOptions;
import java.util.*;
import javax.validation.*;
import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

/** DataflowRunnerConfig contains configuration fields for the Dataflow job runner. */
@Getter
@Setter
public class DataflowRunnerConfig extends RunnerConfig {

  public DataflowRunnerConfig(DataflowRunnerConfigOptions runnerConfigOptions) {
    this.project = runnerConfigOptions.getProject();
    this.region = runnerConfigOptions.getRegion();
    this.zone = runnerConfigOptions.getZone();
    this.serviceAccount = runnerConfigOptions.getServiceAccount();
    this.network = runnerConfigOptions.getNetwork();
    this.subnetwork = runnerConfigOptions.getSubnetwork();
    this.workerMachineType = runnerConfigOptions.getWorkerMachineType();
    this.autoscalingAlgorithm = runnerConfigOptions.getAutoscalingAlgorithm();
    this.usePublicIps = runnerConfigOptions.getUsePublicIps();
    this.tempLocation = runnerConfigOptions.getTempLocation();
    this.maxNumWorkers = runnerConfigOptions.getMaxNumWorkers();
    this.deadLetterTableSpec = runnerConfigOptions.getDeadLetterTableSpec();
    this.diskSizeGb = runnerConfigOptions.getDiskSizeGb();
    this.labels = runnerConfigOptions.getLabelsMap();
    validate();
  }

  /* Project id to use when launching jobs. */
  @NotBlank public String project;

  /* The Google Compute Engine region for creating Dataflow jobs. */
  @NotBlank public String region;

  /* GCP availability zone for operations. */
  @NotBlank public String zone;

  /* Run the job as a specific service account, instead of the default GCE robot. */
  public String serviceAccount;

  /* GCE network for launching workers. */
  @NotBlank public String network;

  /* GCE subnetwork for launching workers. */
  @NotBlank public String subnetwork;

  /* Machine type to create Dataflow worker VMs as. */
  public String workerMachineType;

  /* The autoscaling algorithm to use for the workerpool. */
  public String autoscalingAlgorithm;

  /* Specifies whether worker pools should be started with public IP addresses. */
  public Boolean usePublicIps;

  /**
   * A pipeline level default location for storing temporary files. Support Google Cloud Storage
   * locations, e.g. gs://bucket/object
   */
  @NotBlank public String tempLocation;

  /* The maximum number of workers to use for the workerpool. */
  public Integer maxNumWorkers;

  /* BigQuery table specification, e.g. PROJECT_ID:DATASET_ID.PROJECT_ID */
  public String deadLetterTableSpec;

  /* Disk size to use on each remote Compute Engine worker instance */
  public Integer diskSizeGb;

  public Map<String, String> labels;

  /** Validates Dataflow runner configuration options */
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
