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
	"feast/cli/feast/pkg/util"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/gojektech/feast/go-feast-proto/feast/core"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/spf13/cobra"
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list [resource]",
	Short: "List the resources currently registered to feast.",
	Long: `Prints a list of all of the resource type currently registered to feast.
	
Valid resources include:
- entities
- features
- storage
- jobs

Examples:
- feast list entities`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		if len(args) != 1 {
			return errors.New("invalid number of arguments for list command")
		}

		initConn()
		err := list(args[0])
		if err != nil {
			return fmt.Errorf("failed to list %s: %v", args[0], err)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(listCmd)
}

func list(resource string) error {
	ctx := context.Background()

	switch resource {
	case "features":
		return listFeatures(ctx, core.NewCoreServiceClient(coreConn))
	case "entities":
		return listEntities(ctx, core.NewCoreServiceClient(coreConn))
	case "storage":
		return listStorage(ctx, core.NewCoreServiceClient(coreConn))
	case "jobs":
		return listJobs(ctx, core.NewJobServiceClient(coreConn))
	default:
		return fmt.Errorf("invalid resource %s: please choose one of [features, entities, storage, jobs]", resource)
	}
}

func listFeatures(ctx context.Context, cli core.CoreServiceClient) error {
	response, err := cli.ListFeatures(ctx, &empty.Empty{})
	if err != nil {
		return err
	}
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(w, "ID\tDESCRIPTION\tOWNER\n")
	fmt.Fprintf(w, "--\t-----------\t-----\n")
	for _, feat := range response.GetFeatures() {
		fmt.Fprintf(w, strings.Join(
			[]string{feat.Id, feat.Description, feat.Owner}, "\t")+"\n")
	}
	w.Flush()
	return nil
}

func listEntities(ctx context.Context, cli core.CoreServiceClient) error {
	response, err := cli.ListEntities(ctx, &empty.Empty{})
	if err != nil {
		return err
	}
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(w, "ID\tDESCRIPTION\n")
	fmt.Fprintf(w, "--\t-----------\t\n")
	for _, ent := range response.GetEntities() {
		fmt.Fprintf(w, strings.Join(
			[]string{ent.Name, ent.Description}, "\t")+"\n")
	}
	w.Flush()
	return nil
}

func listStorage(ctx context.Context, cli core.CoreServiceClient) error {
	response, err := cli.ListStorage(ctx, &empty.Empty{})
	if err != nil {
		return err
	}
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(w, "ID\tTYPE\n")
	fmt.Fprintf(w, "--\t----\t\n")
	for _, feat := range response.GetStorageSpecs() {
		fmt.Fprintf(w, strings.Join(
			[]string{feat.Id, feat.Type}, "\t")+"\n")
	}
	w.Flush()
	return nil
}

func listJobs(ctx context.Context, cli core.JobServiceClient) error {
	response, err := cli.ListJobs(ctx, &empty.Empty{})
	if err != nil {
		return err
	}
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(w, "JOB_ID\tTYPE\tRUNNER\tSTATUS\tAGE\n")
	fmt.Fprintf(w, "------\t----\t------\t------\t---\n")
	for _, job := range response.GetJobs() {
		fmt.Fprintf(w, strings.Join(
			[]string{job.Id, job.Type, job.Runner, job.Status, util.ParseAge(*job.Created)}, "\t")+"\n")
	}
	w.Flush()
	return nil
}
