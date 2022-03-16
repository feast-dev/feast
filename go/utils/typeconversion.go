package utils

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func ProtoTypeToArrowType(sample *types.Value) (arrow.DataType, error) {
	switch sample.Val.(type) {
	case *types.Value_BytesVal:
		return arrow.FixedWidthTypes.Boolean, nil
	case *types.Value_Int32Val:
		return arrow.PrimitiveTypes.Int32, nil
	case *types.Value_Int64Val:
		return arrow.PrimitiveTypes.Int64, nil
	case *types.Value_FloatVal:
		return arrow.PrimitiveTypes.Float32, nil
	case *types.Value_DoubleVal:
		return arrow.PrimitiveTypes.Float64, nil
	default:
		return nil,
			fmt.Errorf("unsupported proto type in proto to arrow conversion: %s", sample.Val)
	}
}

func ProtoValuesToArrowArray(builder array.Builder, values []*types.Value) error {
	switch fieldBuilder := builder.(type) {
	case *array.BooleanBuilder:
		for _, v := range values {
			fieldBuilder.Append(v.GetBoolVal())
		}
	case *array.Int32Builder:
		for _, v := range values {
			fieldBuilder.Append(v.GetInt32Val())
		}
	case *array.Int64Builder:
		for _, v := range values {
			fieldBuilder.Append(v.GetInt64Val())
		}
	case *array.Float32Builder:
		for _, v := range values {
			fieldBuilder.Append(v.GetFloatVal())
		}
	case *array.Float64Builder:
		for _, v := range values {
			fieldBuilder.Append(v.GetDoubleVal())
		}
	default:
		return fmt.Errorf("unsupported array builder: %s", builder)
	}
	return nil
}

func ArrowValuesToProtoValues(arr array.Interface) ([]*types.Value, error) {
	values := make([]*types.Value, 0)
	switch arr.DataType() {
	case arrow.PrimitiveTypes.Int32:
		for _, v := range arr.(*array.Int32).Int32Values() {
			values = append(values, &types.Value{Val: &types.Value_Int32Val{Int32Val: v}})
		}
	case arrow.PrimitiveTypes.Int64:
		for _, v := range arr.(*array.Int64).Int64Values() {
			values = append(values, &types.Value{Val: &types.Value_Int64Val{Int64Val: v}})
		}
	case arrow.PrimitiveTypes.Float32:
		for _, v := range arr.(*array.Float32).Float32Values() {
			values = append(values, &types.Value{Val: &types.Value_FloatVal{FloatVal: v}})
		}
	case arrow.FixedWidthTypes.Boolean:
		for idx := 0; idx < arr.Len(); idx++ {
			values = append(values,
				&types.Value{Val: &types.Value_BoolVal{BoolVal: arr.(*array.Boolean).Value(idx)}})
		}
	default:
		return nil, fmt.Errorf("unsupported arrow to proto conversion for type %s", arr.DataType())
	}

	return values, nil
}

func protoTypeToArrowType(sample *types.Value) (arrow.DataType, error) {
	switch sample.Val.(type) {
	case *types.Value_BytesVal:
		return arrow.FixedWidthTypes.Boolean, nil
	case *types.Value_Int32Val:
		return arrow.PrimitiveTypes.Int32, nil
	case *types.Value_Int64Val:
		return arrow.PrimitiveTypes.Int64, nil
	case *types.Value_FloatVal:
		return arrow.PrimitiveTypes.Float32, nil
	case *types.Value_DoubleVal:
		return arrow.PrimitiveTypes.Float64, nil
	default:
		return nil,
			fmt.Errorf("unsupported proto type in proto to arrow conversion: %s", sample.Val)
	}
}
