package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/gojek/feast/cli/feast/pkg/printer"
	"github.com/gojek/feast/protos/generated/go/feast/core"
	"github.com/spf13/cobra"
)

// listCmd represents the list command
var getCmd = &cobra.Command{
	Use:   "get [resource] [id]",
	Short: "Get and print the details of the desired resource.",
	Long: `Get and print the details of the desired resource.
	
Valid resources include:
- entity
- feature
- storage
- job

Examples:
- feast get entity myentity`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		if len(args) != 2 {
			return errors.New("invalid number of arguments for list command")
		}

		initConn()
		err := get(args[0], args[1])
		if err != nil {
			return fmt.Errorf("failed to list %s: %v", args[0], err)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
}

func get(resource string, id string) error {
	ctx := context.Background()

	switch resource {
	case "feature":
		return getFeature(ctx, core.NewUIServiceClient(coreConn), id)
	case "entity":
		return getEntity(ctx, core.NewUIServiceClient(coreConn), id)
	case "storage":
		return getStorage(ctx, core.NewUIServiceClient(coreConn), id)
	case "job":
		return getJob(ctx, core.NewJobServiceClient(coreConn), id)
	default:
		return fmt.Errorf("invalid resource %s: please choose one of [features, entities, storage, jobs]", resource)
	}
}

func getFeature(ctx context.Context, cli core.UIServiceClient, id string) error {
	response, err := cli.GetFeature(ctx, &core.UIServiceTypes_GetFeatureRequest{Id: id})
	if err != nil {
		return err
	}
	printer.PrintFeatureDetail(response.GetFeature())
	return nil
}

func getEntity(ctx context.Context, cli core.UIServiceClient, id string) error {
	response, err := cli.GetEntity(ctx, &core.UIServiceTypes_GetEntityRequest{Id: id})
	if err != nil {
		return err
	}
	printer.PrintEntityDetail(response.GetEntity())
	return nil
}

func getStorage(ctx context.Context, cli core.UIServiceClient, id string) error {
	response, err := cli.GetStorage(ctx, &core.UIServiceTypes_GetStorageRequest{Id: id})
	if err != nil {
		return err
	}
	printer.PrintStorageDetail(response.GetStorage())
	return nil
}

func getJob(ctx context.Context, cli core.JobServiceClient, id string) error {
	response, err := cli.GetJob(ctx, &core.JobServiceTypes_GetJobRequest{Id: id})
	if err != nil {
		return err
	}
	printer.PrintJobDetail(response.GetJob())
	return nil
}
