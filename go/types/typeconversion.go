package types

import (
	"fmt"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"

	"github.com/feast-dev/feast/go/protos/feast/types"
)

func ProtoTypeToArrowType(sample *types.Value) (arrow.DataType, error) {
	switch sample.Val.(type) {
	case *types.Value_BytesVal:
		return arrow.BinaryTypes.Binary, nil
	case *types.Value_StringVal:
		return arrow.BinaryTypes.String, nil
	case *types.Value_Int32Val:
		return arrow.PrimitiveTypes.Int32, nil
	case *types.Value_Int64Val:
		return arrow.PrimitiveTypes.Int64, nil
	case *types.Value_FloatVal:
		return arrow.PrimitiveTypes.Float32, nil
	case *types.Value_DoubleVal:
		return arrow.PrimitiveTypes.Float64, nil
	case *types.Value_BoolVal:
		return arrow.FixedWidthTypes.Boolean, nil
	case *types.Value_BoolListVal:
		return arrow.ListOf(arrow.FixedWidthTypes.Boolean), nil
	case *types.Value_StringListVal:
		return arrow.ListOf(arrow.BinaryTypes.String), nil
	case *types.Value_BytesListVal:
		return arrow.ListOf(arrow.BinaryTypes.Binary), nil
	case *types.Value_Int32ListVal:
		return arrow.ListOf(arrow.PrimitiveTypes.Int32), nil
	case *types.Value_Int64ListVal:
		return arrow.ListOf(arrow.PrimitiveTypes.Int64), nil
	case *types.Value_FloatListVal:
		return arrow.ListOf(arrow.PrimitiveTypes.Float32), nil
	case *types.Value_DoubleListVal:
		return arrow.ListOf(arrow.PrimitiveTypes.Float64), nil
	case *types.Value_UnixTimestampVal:
		return arrow.FixedWidthTypes.Timestamp_s, nil
	case *types.Value_UnixTimestampListVal:
		return arrow.ListOf(arrow.FixedWidthTypes.Timestamp_s), nil
	default:
		return nil,
			fmt.Errorf("unsupported proto type in proto to arrow conversion: %s", sample.Val)
	}
}

func ValueTypeEnumToArrowType(t types.ValueType_Enum) (arrow.DataType, error) {
	switch t {
	case types.ValueType_BYTES:
		return arrow.BinaryTypes.Binary, nil
	case types.ValueType_STRING:
		return arrow.BinaryTypes.String, nil
	case types.ValueType_INT32:
		return arrow.PrimitiveTypes.Int32, nil
	case types.ValueType_INT64:
		return arrow.PrimitiveTypes.Int64, nil
	case types.ValueType_FLOAT:
		return arrow.PrimitiveTypes.Float32, nil
	case types.ValueType_DOUBLE:
		return arrow.PrimitiveTypes.Float64, nil
	case types.ValueType_BOOL:
		return arrow.FixedWidthTypes.Boolean, nil
	case types.ValueType_BOOL_LIST:
		return arrow.ListOf(arrow.FixedWidthTypes.Boolean), nil
	case types.ValueType_STRING_LIST:
		return arrow.ListOf(arrow.BinaryTypes.String), nil
	case types.ValueType_BYTES_LIST:
		return arrow.ListOf(arrow.BinaryTypes.Binary), nil
	case types.ValueType_INT32_LIST:
		return arrow.ListOf(arrow.PrimitiveTypes.Int32), nil
	case types.ValueType_INT64_LIST:
		return arrow.ListOf(arrow.PrimitiveTypes.Int64), nil
	case types.ValueType_FLOAT_LIST:
		return arrow.ListOf(arrow.PrimitiveTypes.Float32), nil
	case types.ValueType_DOUBLE_LIST:
		return arrow.ListOf(arrow.PrimitiveTypes.Float64), nil
	case types.ValueType_UNIX_TIMESTAMP:
		return arrow.FixedWidthTypes.Timestamp_s, nil
	case types.ValueType_UNIX_TIMESTAMP_LIST:
		return arrow.ListOf(arrow.FixedWidthTypes.Timestamp_s), nil
	default:
		return nil,
			fmt.Errorf("unsupported value type enum in enum to arrow type conversion: %s", t)
	}
}

func CopyProtoValuesToArrowArray(builder array.Builder, values []*types.Value) error {
	switch fieldBuilder := builder.(type) {
	case *array.BooleanBuilder:
		for _, v := range values {
			fieldBuilder.Append(v.GetBoolVal())
		}
	case *array.BinaryBuilder:
		for _, v := range values {
			fieldBuilder.Append(v.GetBytesVal())
		}
	case *array.StringBuilder:
		for _, v := range values {
			fieldBuilder.Append(v.GetStringVal())
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
	case *array.TimestampBuilder:
		for _, v := range values {
			fieldBuilder.Append(arrow.Timestamp(v.GetUnixTimestampVal()))
		}
	case *array.ListBuilder:
		for _, list := range values {
			fieldBuilder.Append(true)

			switch valueBuilder := fieldBuilder.ValueBuilder().(type) {

			case *array.BooleanBuilder:
				for _, v := range list.GetBoolListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.BinaryBuilder:
				for _, v := range list.GetBytesListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.StringBuilder:
				for _, v := range list.GetStringListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Int32Builder:
				for _, v := range list.GetInt32ListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Int64Builder:
				for _, v := range list.GetInt64ListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Float32Builder:
				for _, v := range list.GetFloatListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Float64Builder:
				for _, v := range list.GetDoubleListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.TimestampBuilder:
				for _, v := range list.GetUnixTimestampListVal().GetVal() {
					valueBuilder.Append(arrow.Timestamp(v))
				}
			}
		}
	default:
		return fmt.Errorf("unsupported array builder: %s", builder)
	}
	return nil
}

func ArrowValuesToProtoValues(arr arrow.Array) ([]*types.Value, error) {
	values := make([]*types.Value, 0)

	if listArr, ok := arr.(*array.List); ok {
		listValues := listArr.ListValues()
		offsets := listArr.Offsets()[1:]
		pos := 0
		for idx := 0; idx < listArr.Len(); idx++ {
			switch listValues.DataType() {
			case arrow.PrimitiveTypes.Int32:
				vals := make([]int32, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = listValues.(*array.Int32).Value(j)
				}
				values = append(values,
					&types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: vals}}})
			case arrow.PrimitiveTypes.Int64:
				vals := make([]int64, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = listValues.(*array.Int64).Value(j)
				}
				values = append(values,
					&types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: vals}}})
			case arrow.PrimitiveTypes.Float32:
				vals := make([]float32, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = listValues.(*array.Float32).Value(j)
				}
				values = append(values,
					&types.Value{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: vals}}})
			case arrow.PrimitiveTypes.Float64:
				vals := make([]float64, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = listValues.(*array.Float64).Value(j)
				}
				values = append(values,
					&types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: vals}}})
			case arrow.BinaryTypes.Binary:
				vals := make([][]byte, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = listValues.(*array.Binary).Value(j)
				}
				values = append(values,
					&types.Value{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: vals}}})
			case arrow.BinaryTypes.String:
				vals := make([]string, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = listValues.(*array.String).Value(j)
				}
				values = append(values,
					&types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: vals}}})
			case arrow.FixedWidthTypes.Boolean:
				vals := make([]bool, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = listValues.(*array.Boolean).Value(j)
				}
				values = append(values,
					&types.Value{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: vals}}})
			case arrow.FixedWidthTypes.Timestamp_s:
				vals := make([]int64, int(offsets[idx])-pos)
				for j := pos; j < int(offsets[idx]); j++ {
					vals[j-pos] = int64(listValues.(*array.Timestamp).Value(j))
				}

				values = append(values,
					&types.Value{Val: &types.Value_UnixTimestampListVal{
						UnixTimestampListVal: &types.Int64List{Val: vals}}})

			}

			// set the end of current element as start of the next
			pos = int(offsets[idx])
		}

		return values, nil
	}

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
	case arrow.PrimitiveTypes.Float64:
		for _, v := range arr.(*array.Float64).Float64Values() {
			values = append(values, &types.Value{Val: &types.Value_DoubleVal{DoubleVal: v}})
		}
	case arrow.FixedWidthTypes.Boolean:
		for idx := 0; idx < arr.Len(); idx++ {
			values = append(values,
				&types.Value{Val: &types.Value_BoolVal{BoolVal: arr.(*array.Boolean).Value(idx)}})
		}
	case arrow.BinaryTypes.Binary:
		for idx := 0; idx < arr.Len(); idx++ {
			values = append(values,
				&types.Value{Val: &types.Value_BytesVal{BytesVal: arr.(*array.Binary).Value(idx)}})
		}
	case arrow.BinaryTypes.String:
		for idx := 0; idx < arr.Len(); idx++ {
			values = append(values,
				&types.Value{Val: &types.Value_StringVal{StringVal: arr.(*array.String).Value(idx)}})
		}
	case arrow.FixedWidthTypes.Timestamp_s:
		for idx := 0; idx < arr.Len(); idx++ {
			values = append(values,
				&types.Value{Val: &types.Value_UnixTimestampVal{
					UnixTimestampVal: int64(arr.(*array.Timestamp).Value(idx))}})
		}
	case arrow.Null:
		for idx := 0; idx < arr.Len(); idx++ {
			values = append(values, &types.Value{})
		}
	default:
		return nil, fmt.Errorf("unsupported arrow to proto conversion for type %s", arr.DataType())
	}

	return values, nil
}

func ProtoValuesToArrowArray(protoValues []*types.Value, arrowAllocator memory.Allocator, numRows int) (arrow.Array, error) {
	var fieldType arrow.DataType
	var err error

	for _, val := range protoValues {
		if val != nil {
			fieldType, err = ProtoTypeToArrowType(val)
			if err != nil {
				return nil, err
			}
			break
		}
	}

	if fieldType != nil {
		builder := array.NewBuilder(arrowAllocator, fieldType)
		err = CopyProtoValuesToArrowArray(builder, protoValues)
		if err != nil {
			return nil, err
		}

		return builder.NewArray(), nil
	} else {
		return array.NewNull(numRows), nil
	}
}
