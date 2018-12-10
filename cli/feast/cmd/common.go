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
	"fmt"

	"google.golang.org/grpc"
)

// global variables
var (
	cfg      *cliCfg
	coreConn *grpc.ClientConn
)

func initConn() {
	var err error
	coreConn, err = grpc.Dial(cfg.CoreURI, grpc.WithInsecure())
	if err != nil {
		handleErr(fmt.Errorf("unable to connect to core service at %s: %v", cfg.CoreURI, err))
	}
}

func closeConn() {
	if coreConn != nil {
		coreConn.Close()
	}
}
