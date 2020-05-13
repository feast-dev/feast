package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"github.com/gojek/feast/sdk/go/protos/feast/types"
	json "github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"testing"
)

func TestGetOnlineFeaturesRequest(t *testing.T) {
	tt := []struct {
		name    string
		req     OnlineFeaturesRequest
		want    *serving.GetOnlineFeaturesRequest
		wantErr bool
		err     error
	}{
		{
			name: "valid",
			req: OnlineFeaturesRequest{
				Features: []string{"my_project_1/feature1", "my_project_2/feature1", "my_project_4/feature3", "feature2", "feature2"},
				Entities: []Row{
					{"entity1": Int64Val(1), "entity2": StrVal("bob")},
					{"entity1": Int64Val(1), "entity2": StrVal("annie")},
					{"entity1": Int64Val(1), "entity2": StrVal("jane")},
				},
				Project: "my_project_3",
			},
			want: &serving.GetOnlineFeaturesRequest{
				Features: []*serving.FeatureReference{
					{
						Project: "my_project_1",
						Name:    "feature1",
					},
					{
						Project: "my_project_2",
						Name:    "feature1",
					},
					{
						Project: "my_project_4",
						Name:    "feature3",
					},
					{
						Project: "my_project_3",
						Name:    "feature2",
					},
					{
						Project: "my_project_3",
						Name:    "feature2",
					},
				},
				EntityRows: []*serving.GetOnlineFeaturesRequest_EntityRow{
					{
						Fields: map[string]*types.Value{
							"entity1": Int64Val(1),
							"entity2": StrVal("bob"),
						},
					},
					{
						Fields: map[string]*types.Value{
							"entity1": Int64Val(1),
							"entity2": StrVal("annie"),
						},
					},
					{
						Fields: map[string]*types.Value{
							"entity1": Int64Val(1),
							"entity2": StrVal("jane"),
						},
					},
				},
				OmitEntitiesInResponse: false,
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "valid_project_in_name",
			req: OnlineFeaturesRequest{
				Features: []string{"project/feature1"},
				Entities: []Row{},
			},
			want: &serving.GetOnlineFeaturesRequest{
				Features: []*serving.FeatureReference{
					{
						Project: "project",
						Name:    "feature1",
					},
				},
				EntityRows:             []*serving.GetOnlineFeaturesRequest_EntityRow{},
				OmitEntitiesInResponse: false,
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "no_project",
			req: OnlineFeaturesRequest{
				Features: []string{"feature1"},
				Entities: []Row{},
			},
			wantErr: true,
			err:     fmt.Errorf(ErrInvalidFeatureName, "feature1"),
		},
		{
			name: "invalid_feature_name/wrong_format",
			req: OnlineFeaturesRequest{
				Features: []string{"/fs1:feature1"},
				Entities: []Row{},
				Project:  "my_project",
			},
			wantErr: true,
			err:     fmt.Errorf(ErrInvalidFeatureName, "/fs1:feature1"),
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.req.buildRequest()

			if (err != nil) != tc.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr && err.Error() != tc.err.Error() {
				t.Errorf("error = %v, expected err = %v", err, tc.err)
				return
			}

			if !proto.Equal(got, tc.want) {
				m := json.Marshaler{}
				gotJSON, _ := m.MarshalToString(got)
				wantJSON, _ := m.MarshalToString(tc.want)
				t.Errorf("got: \n%v\nwant:\n%v", gotJSON, wantJSON)
			}
		})
	}
}
