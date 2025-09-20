package onlinestore

import (
	"testing"
	"reflect"
	"github.com/stretchr/testify/assert"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func Test_serializeEntityKey(t *testing.T) {
	expect_res := []byte{1, 0, 0, 0, 2, 0, 0, 0, 9, 0, 0, 0, 100, 114, 105, 118, 101, 114, 95, 105, 100, 4, 0, 0, 0, 8, 0, 0, 0, 233, 3, 0, 0, 0, 0, 0, 0}
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		entityKey                     *types.EntityKey
		entityKeySerializationVersion int64
		want                          []byte
		wantErr                       bool
	}{
		{
			"test a specific key",
			&types.EntityKey{
				JoinKeys:     []string{"driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}},
			},
			3,
			expect_res,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := serializeEntityKey(tt.entityKey, tt.entityKeySerializationVersion)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("serializeEntityKey() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("serializeEntityKey() succeeded unexpectedly")
			}
			assert.True(t, reflect.DeepEqual(*got, tt.want))
		})
	}
}
