package feast

import (
	"fmt"
	"github.com/gojek/feast/sdk/go/protos/feast/serving"
	"github.com/gojek/feast/sdk/go/protos/feast/types"
	"github.com/google/go-cmp/cmp"
	"testing"
)

var response = OnlineFeaturesResponse{
	RawResponse: &serving.GetOnlineFeaturesResponse{
		FieldValues: []*serving.GetOnlineFeaturesResponse_FieldValues{
			{
				Fields: map[string]*types.Value{
					"project1/feature1": Int64Val(1),
					"project1/feature2": {},
				},
			},
			{
				Fields: map[string]*types.Value{
					"project1/feature1": Int64Val(2),
					"project1/feature2": Int64Val(2),
				},
			},
		},
	},
}

func TestOnlineFeaturesResponseToRow(t *testing.T) {
	actual := response.Rows()
	expected := []Row{
		{"project1/feature1": Int64Val(1), "project1/feature2": &types.Value{}},
		{"project1/feature1": Int64Val(2), "project1/feature2": Int64Val(2)},
	}
	if len(expected) != len(actual) {
		t.Errorf("expected: %v, got: %v", expected, actual)
	}
	for i := range expected {
		if !expected[i].equalTo(actual[i]) {
			t.Errorf("expected: %v, got: %v", expected, actual)
		}
	}
}

func TestOnlineFeaturesResponseToInt64Array(t *testing.T) {
	type args struct {
		order  []string
		fillNa []int64
	}
	tt := []struct {
		name    string
		args    args
		want    [][]int64
		wantErr bool
		err     error
	}{
		{
			name: "valid",
			args: args{
				order:  []string{"project1/feature2", "project1/feature1"},
				fillNa: []int64{-1, -1},
			},
			want:    [][]int64{{-1, 1}, {2, 2}},
			wantErr: false,
		},
		{
			name: "length mismatch",
			args: args{
				order:  []string{"fs:feature2", "fs:feature1"},
				fillNa: []int64{-1},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf(ErrLengthMismatch, 1, 2),
		},
		{
			name: "length mismatch",
			args: args{
				order:  []string{"project1/feature2", "project1/feature3"},
				fillNa: []int64{-1, -1},
			},
			want:    nil,
			wantErr: true,
			err:     fmt.Errorf(ErrFeatureNotFound, "project1/feature3"),
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got, err := response.Int64Arrays(tc.args.order, tc.args.fillNa)
			if (err != nil) != tc.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if tc.wantErr && err.Error() != tc.err.Error() {
				t.Errorf("error = %v, expected err = %v", err, tc.err)
				return
			}
			if !cmp.Equal(got, tc.want) {
				t.Errorf("got: \n%v\nwant:\n%v", got, tc.want)
			}
		})
	}
}
