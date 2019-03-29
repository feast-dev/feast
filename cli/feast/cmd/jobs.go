// Copyright 2018 The Feast Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/gojek/feast/cli/feast/pkg/parse"
	"github.com/gojek/feast/protos/generated/go/feast/core"

	"github.com/spf13/cobra"
)

var (
	waitJobComplete = false
<<<<<<< HEAD
=======
	jobName         = "feastimport"
>>>>>>> 79abea3dde21c2856d23cba8866a1e06b6166939
)

// jobsCmd represents the jobs command
var jobsCmd = &cobra.Command{
	Use:   "jobs",
	Short: "Jobs utilities for feast",
}

var jobsRunCmd = &cobra.Command{
	Use:   "run [filepath]",
	Short: "Submit a job to the jobservice and run it",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		if len(args) > 1 {
			return errors.New("invalid number of arguments for jobs run command")
		}
		ctx := context.Background()
		return runJob(ctx, args[0])
	},
}

var jobsAbortCmd = &cobra.Command{
	Use:   "stop [job_id]",
	Short: "Stop the given job",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		if len(args) > 1 {
			return errors.New("invalid number of arguments for jobs stop command")
		}
		ctx := context.Background()
		return abortJob(ctx, args[0])
	},
}

func init() {
	jobsRunCmd.Flags().BoolVar(&waitJobComplete, "wait", false, "wait for job to run to completion")
	jobsRunCmd.Flags().StringVar(&jobName, "name", "feastimport", "job name to be submitted")
	jobsCmd.AddCommand(jobsRunCmd)
	jobsCmd.AddCommand(jobsAbortCmd)
	rootCmd.AddCommand(jobsCmd)
}

func runJob(ctx context.Context, path string) error {
	d, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("[jobs] could not read file: %v", err)
	}
	is, err := parse.YamlToImportSpec(d)
	if err != nil {
		return fmt.Errorf("[jobs] unable to parse yaml file at %s: %v", path, err)
	}
	initConn()
	jobsClient := core.NewJobServiceClient(coreConn)
	out, err := jobsClient.SubmitJob(ctx, &core.JobServiceTypes_SubmitImportJobRequest{
		Name:       jobName,
		ImportSpec: is,
	})
	if err != nil {
		return fmt.Errorf("[jobs] failed to start job: %v", err)
	}
	fmt.Printf("[jobs] started job with ID: %s", out.GetJobId())
	if waitJobComplete {
		return waitJob(ctx, jobsClient, out.GetJobId())
	}
	return nil
}

func waitJob(ctx context.Context, jobsClient core.JobServiceClient, jobID string) error {
	for {
		response, err := jobsClient.GetJob(ctx, &core.JobServiceTypes_GetJobRequest{
			Id: jobID,
		})
		if err != nil {
			return fmt.Errorf("[jobs] error while querying job id %s: %v", jobID, err)
		}

		status := response.GetJob().GetStatus()
		fmt.Printf("\r[jobs] job id %s is currently: %s\n", jobID, status)
		switch status {
		case "COMPLETED":
			return nil
		case "ABORTED":
			return fmt.Errorf("[jobs] job id %s failed: Job was aborted", jobID)
		case "ERROR":
			return fmt.Errorf("[jobs] job id %s failed: Job terminated with error. For more information, refer to job logs", jobID)
		}
		time.Sleep(5 * time.Second)
	}
}

func abortJob(ctx context.Context, id string) error {
	initConn()
	jobsClient := core.NewJobServiceClient(coreConn)
	response, err := jobsClient.AbortJob(ctx, &core.JobServiceTypes_AbortJobRequest{Id: id})
	if err != nil {
		return err
	}
	fmt.Printf("Aborting job with id: %s\n", response.GetId())
	return nil
}
