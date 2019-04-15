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
	"testing"

	"github.com/gojek/feast/cli/feast/cmd/mock"
	"github.com/golang/mock/gomock"

	"github.com/gojek/feast/protos/generated/go/feast/core"
)

func Test_apply(t *testing.T) {
	mockCore := mock.NewMockCoreServiceClient(gomock.NewController(t))
	mockCore.EXPECT().ApplyEntity(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockCore.EXPECT().ApplyFeature(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockCore.EXPECT().ApplyFeatureGroup(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	type args struct {
		ctx          context.Context
		coreCli      core.CoreServiceClient
		resource     string
		fileLocation string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test apply invalid resource",
			args: args{
				ctx:          context.Background(),
				coreCli:      mockCore,
				resource:     "invalidResource",
				fileLocation: "testdata/valid_entity.yaml",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "test apply entity",
			args: args{
				ctx:          context.Background(),
				coreCli:      mockCore,
				resource:     "entity",
				fileLocation: "testdata/valid_entity.yaml",
			},
			want:    "myentity",
			wantErr: false,
		},
		{
			name: "test apply entity with non-existent file",
			args: args{
				ctx:          context.Background(),
				coreCli:      mockCore,
				resource:     "entity",
				fileLocation: "testdata/file_not_exists.yaml",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "test apply entity with no tag",
			args: args{
				ctx:          context.Background(),
				coreCli:      mockCore,
				resource:     "entity",
				fileLocation: "testdata/valid_entity_no_tag.yaml",
			},
			want:    "myentity",
			wantErr: false,
		},
		{
			name: "test apply invalid syntax in entity yaml",
			args: args{
				ctx:          context.Background(),
				coreCli:      mockCore,
				resource:     "entity",
				fileLocation: "testdata/invalid_entity.yaml",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "test apply feature",
			args: args{
				ctx:          context.Background(),
				coreCli:      mockCore,
				resource:     "feature",
				fileLocation: "testdata/valid_feature.yaml",
			},
			want:    "myentity.feature_bool_redis1",
			wantErr: false,
		},
		{
			name: "test apply feature group",
			args: args{
				ctx:          context.Background(),
				coreCli:      mockCore,
				resource:     "featureGroup",
				fileLocation: "testdata/valid_feature_group.yaml",
			},
			want:    "my_fg",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := apply(tt.args.ctx, tt.args.coreCli, tt.args.resource, tt.args.fileLocation)
			if (err != nil) != tt.wantErr {
				t.Errorf("apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("apply() = %v, want %v", got, tt.want)
			}
		})
	}
}
