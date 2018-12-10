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
	"path/filepath"

	"feast/go-feast-proto/feast/core"

	"feast/cli/feast/pkg/parse"

	"github.com/spf13/cobra"
)

// registerCmd represents the register command
var registerCmd = &cobra.Command{
	Use:   "register [resource] [filepaths...]",
	Short: "Register a resource given one or many yaml files.",
	Long: `Register a resource from one or multiple yamls.
	
Valid resources include:
- entity
- feature
- featureGroup
- storage

Examples:
- feast register entity entity.yml
- feast register storage storage1.yml storage2.yml
- feast register feature *-feature.yml`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		if len(args) < 2 {
			fmt.Println(args)
			return errors.New("invalid number of arguments for register command")
		}

		initConn()
		ctx := context.Background()
		coreCli := core.NewCoreServiceClient(coreConn)
		resource := args[0]
		paths := args[1:]

		for _, fp := range paths {
			if isYaml(fp) {
				fmt.Printf("Registering %s at %s\n", resource, fp)
				regID, err := register(ctx, coreCli, resource, fp)
				if err != nil {
					return fmt.Errorf("failed to register %s at path %s: %v", resource, fp, err)
				}
				fmt.Printf("Successfully registered %s %s\n", resource, regID)
			}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(registerCmd)
}

func register(ctx context.Context, coreCli core.CoreServiceClient, resource string, fileLocation string) (string, error) {
	yml, err := ioutil.ReadFile(fileLocation)
	if err != nil {
		return "", fmt.Errorf("error reading file at %s: %v", fileLocation, err)
	}

	switch resource {
	case "feature":
		return registerFeature(ctx, coreCli, yml)
	case "featureGroup":
		return registerFeatureGroup(ctx, coreCli, yml)
	case "entity":
		return registerEntity(ctx, coreCli, yml)
	case "storage":
		return registerStorage(ctx, coreCli, yml)
	default:
		return "", fmt.Errorf("invalid resource %s: please choose one of [feature, featureGroup, entity, storage]", resource)
	}
}

func registerFeature(ctx context.Context, coreCli core.CoreServiceClient, yml []byte) (string, error) {
	fs, err := parse.YamlToFeatureSpec(yml)
	if err != nil {
		return "", err
	}
	_, err = coreCli.RegisterFeature(ctx, fs)
	return fs.GetId(), err
}

func registerFeatureGroup(ctx context.Context, coreCli core.CoreServiceClient, yml []byte) (string, error) {
	fgs, err := parse.YamlToFeatureGroupSpec(yml)
	if err != nil {
		return "", err
	}
	_, err = coreCli.RegisterFeatureGroup(ctx, fgs)
	return fgs.GetId(), err
}

func registerEntity(ctx context.Context, coreCli core.CoreServiceClient, yml []byte) (string, error) {
	es, err := parse.YamlToEntitySpec(yml)
	if err != nil {
		return "", err
	}
	_, err = coreCli.RegisterEntity(ctx, es)
	return es.GetName(), err
}

func registerStorage(ctx context.Context, coreCli core.CoreServiceClient, yml []byte) (string, error) {
	ss, err := parse.YamlToStorageSpec(yml)
	if err != nil {
		return "", err
	}
	_, err = coreCli.RegisterStorage(ctx, ss)
	return ss.GetId(), err
}

func isYaml(path string) bool {
	ext := filepath.Ext(path)
	if ext == ".yaml" || ext == ".yml" {
		return true
	}
	return false
}
