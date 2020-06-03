package feast

import (
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
	"github.com/golang/protobuf/proto"
)

// Row map of entity values
type Row map[string]*types.Value

func (r Row) equalTo(other Row) bool {
	for k, v := range r {
		if otherV, ok := other[k]; !ok {
			return false
		} else {
			if !proto.Equal(v, otherV) {
				return false
			}
		}
	}
	return true
}

// StrVal is a string type feast value
func StrVal(val string) *types.Value {
	return &types.Value{Val: &types.Value_StringVal{StringVal: val}}
}

// Int32Val is a int32 type feast value
func Int32Val(val int32) *types.Value {
	return &types.Value{Val: &types.Value_Int32Val{Int32Val: val}}
}

// Int64Val is a int64 type feast value
func Int64Val(val int64) *types.Value {
	return &types.Value{Val: &types.Value_Int64Val{Int64Val: val}}
}

// FloatVal is a float32 type feast value
func FloatVal(val float32) *types.Value {
	return &types.Value{Val: &types.Value_FloatVal{FloatVal: val}}
}

// DoubleVal is a float64 type feast value
func DoubleVal(val float64) *types.Value {
	return &types.Value{Val: &types.Value_DoubleVal{DoubleVal: val}}
}

// BoolVal is a bool type feast value
func BoolVal(val bool) *types.Value {
	return &types.Value{Val: &types.Value_BoolVal{BoolVal: val}}
}

// BytesVal is a bytes type feast value
func BytesVal(val []byte) *types.Value {
	return &types.Value{Val: &types.Value_BytesVal{BytesVal: val}}
}
