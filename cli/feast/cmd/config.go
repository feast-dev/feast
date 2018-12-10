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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"gopkg.in/yaml.v2"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

type cliCfg struct {
	CoreURI string `yaml:"coreURI"`
}

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "config utils for feast",
}

var configSetCmd = &cobra.Command{
	Use:   "set [key] [value]",
	Short: "set configuration for the feast cli",
	Long: `set configuration for a given key to the provided value.

Available keys:
  - coreURI: host:port uri to connect to feast core via grpc

Example:
  feast config set coreURI localhost:8433`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		if len(args) > 2 {
			return fmt.Errorf("unable to set config value: %s", "too many arguments provided")
		}
		err := setConfig(args[0], args[1])
		if err != nil {
			return fmt.Errorf("unable to set config value: %s", err)
		}
		return nil
	},
}

var configListCmd = &cobra.Command{
	Use:   "list",
	Short: "list current configuration",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf(`[Current configuration]
coreURI: %s`+"\n", cfg.CoreURI)
		return nil
	},
}

func init() {
	configCmd.AddCommand(configSetCmd)
	configCmd.AddCommand(configListCmd)
	rootCmd.AddCommand(configCmd)
}

func initConfig() {
	home, err := homedir.Dir()
	handleErr(err)

	cfgFile = path.Join(home, ".feast")
	viper.SetConfigType("yaml")
	viper.SetConfigFile(cfgFile)
	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		fmt.Printf("unable to locate configuration at %s. Creating configuration file...\n",
			cfgFile)
		f, err := os.OpenFile(cfgFile, os.O_RDWR|os.O_CREATE, 0666)
		handleErr(err)
		f.Write([]byte("coreURI: \"\""))
		f.Close()
	}
	cfg = &cliCfg{}
	viper.AutomaticEnv() // read in environment variables that match
	if err := viper.ReadInConfig(); err == nil {
		cfg.CoreURI = viper.GetString("coreURI")
	} else {
		handleErr(err)
	}
}

func setConfig(key string, value string) error {
	switch key {
	case "coreURI":
		cfg.CoreURI = value
		fmt.Printf("[config] coreURI set to %s.\n", value)
	default:
		return errors.New("invalid key provided. Available keys: [coreURI]")
	}
	d, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("error writing to config file: %v", err)
	}
	err = ioutil.WriteFile(cfgFile, d, 0644)
	return err
}
