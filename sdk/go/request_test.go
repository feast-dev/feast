package feast

import (
	"fmt"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
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
				Features: []string{
					"driver:driver_id",
				},
				Entities: []Row{
					{"entity1": Int64Val(1), "entity2": StrVal("bob")},
					{"entity1": Int64Val(1), "entity2": StrVal("annie")},
					{"entity1": Int64Val(1), "entity2": StrVal("jane")},
				},
				Project: "driver_project",
			},
			want: &serving.GetOnlineFeaturesRequest{
				Kind: &serving.GetOnlineFeaturesRequest_Features{
					Features: &serving.FeatureList{
						Val: []string{"driver:driver_id"},
					},
				},
				Entities: map[string]*types.RepeatedValue{
					"entity1": &types.RepeatedValue{
						Val: []*types.Value{
							Int64Val(1), Int64Val(1), Int64Val(1),
						},
					},
					"entity2": &types.RepeatedValue{
						Val: []*types.Value{
							StrVal("bob"), StrVal("annie"), StrVal("jane"),
						},
					},
				},
			},
			wantErr: false,
			err:     nil,
		},
		{
			name: "invalid_feature_name/wrong_format",
			req: OnlineFeaturesRequest{
				Features: []string{"/fs1:feature1"},
				Entities: []Row{},
				Project:  "my_project",
			},
			wantErr: true,
			err:     fmt.Errorf(ErrInvalidFeatureRef, "/fs1:feature1"),
		},
		{
			name: "invalid_feature_name",
			req: OnlineFeaturesRequest{
				Features: []string{"feature1"},
				Entities: []Row{},
				Project:  "my_project",
			},
			wantErr: true,
			err:     fmt.Errorf(ErrInvalidFeatureRef, "feature1"),
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
