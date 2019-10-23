package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"github.com/gojek/feast/sdk/go/protos/feast/types"
	json "github.com/golang/protobuf/jsonpb"
	"github.com/google/go-cmp/cmp"
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
				Features:      []string{"fs:1:feature1", "fs:1:feature2", "fs:2:feature1"},
				Entities: []Row{
					{"entity1": Int64Val(1), "entity2": StrVal("bob")},
					{"entity1": Int64Val(1), "entity2": StrVal("annie")},
					{"entity1": Int64Val(1), "entity2": StrVal("jane")},
				},
			},
			want: &serving.GetOnlineFeaturesRequest{
				FeatureSets: []*serving.FeatureSetRequest{
					{
						Name:         "fs",
						Version:      1,
						FeatureNames: []string{"feature1", "feature2"},
					},
					{
						Name:         "fs",
						Version:      2,
						FeatureNames: []string{"feature1"},
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
			err: nil,
		},
		{
			name: "invalid_feature_name/wrong_format",
			req: OnlineFeaturesRequest{
				Features:      []string{"fs1:feature1"},
				Entities:      []Row{},
			},
			wantErr: true,
			err: fmt.Errorf(ErrInvalidFeatureName, "fs1:feature1"),
		},
		{
			name: "invalid_feature_name/invalid_version",
			req: OnlineFeaturesRequest{
				Features:      []string{"fs:a:feature1"},
				Entities:      []Row{},
			},
			wantErr: true,
			err: fmt.Errorf(ErrInvalidFeatureName, "fs:a:feature1"),
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
			if !cmp.Equal(got, tc.want) {
				m := json.Marshaler{}
				gotJson, _ := m.MarshalToString(got)
				wantJson, _ := m.MarshalToString(tc.want)
				t.Errorf("got: \n%v\nwant:\n%v", gotJson, wantJson)
			}
		})
	}
}
