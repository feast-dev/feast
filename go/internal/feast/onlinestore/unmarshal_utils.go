package onlinestore

import (
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"google.golang.org/protobuf/proto"
	"math"
)

func UnmarshalStoredProto(valueStr []byte) (*types.Value, serving.FieldStatus, error) {
	var message types.Value
	null := &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}
	if err := proto.Unmarshal(valueStr, &message); err != nil {
		return nil, serving.FieldStatus_INVALID, fmt.Errorf("error converting parsed online store Value to types.Value: %w", err)
	}
	if message.Val == nil {
		return null, serving.FieldStatus_NULL_VALUE, nil
	} else {
		switch message.Val.(type) {
		case *types.Value_UnixTimestampVal:
			// null timestamps are read as min int64, so we convert them to nil
			if message.Val.(*types.Value_UnixTimestampVal).UnixTimestampVal == math.MinInt64 {
				return null, serving.FieldStatus_NULL_VALUE, nil
			} else {
				return &types.Value{Val: message.Val}, serving.FieldStatus_PRESENT, nil
			}
		default:
			return &types.Value{Val: message.Val}, serving.FieldStatus_PRESENT, nil
		}
	}
}
