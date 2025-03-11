package types

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func ProtoTypeToArrowType(sample *types.Value) (arrow.DataType, error) {
	if sample.Val == nil {
		return nil, nil
	}
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
	for _, value := range values {
		if value == nil || value.Val == nil {
			builder.AppendNull()
			continue
		}

		switch fieldBuilder := builder.(type) {

		case *array.BooleanBuilder:
			fieldBuilder.Append(value.GetBoolVal())
		case *array.BinaryBuilder:
			fieldBuilder.Append(value.GetBytesVal())
		case *array.StringBuilder:
			fieldBuilder.Append(value.GetStringVal())
		case *array.Int32Builder:
			fieldBuilder.Append(value.GetInt32Val())
		case *array.Int64Builder:
			fieldBuilder.Append(value.GetInt64Val())
		case *array.Float32Builder:
			fieldBuilder.Append(value.GetFloatVal())
		case *array.Float64Builder:
			fieldBuilder.Append(value.GetDoubleVal())
		case *array.TimestampBuilder:
			fieldBuilder.Append(arrow.Timestamp(value.GetUnixTimestampVal()))
		case *array.ListBuilder:
			fieldBuilder.Append(true)

			switch valueBuilder := fieldBuilder.ValueBuilder().(type) {

			case *array.BooleanBuilder:
				for _, v := range value.GetBoolListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.BinaryBuilder:
				for _, v := range value.GetBytesListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.StringBuilder:
				for _, v := range value.GetStringListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Int32Builder:
				for _, v := range value.GetInt32ListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Int64Builder:
				for _, v := range value.GetInt64ListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Float32Builder:
				for _, v := range value.GetFloatListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.Float64Builder:
				for _, v := range value.GetDoubleListVal().GetVal() {
					valueBuilder.Append(v)
				}
			case *array.TimestampBuilder:
				for _, v := range value.GetUnixTimestampListVal().GetVal() {
					valueBuilder.Append(arrow.Timestamp(v))
				}
			}
		default:
			return fmt.Errorf("unsupported array builder: %s", builder)
		}
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
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_Int32Val{Int32Val: arr.(*array.Int32).Value(idx)}})
			}
		}
	case arrow.PrimitiveTypes.Int64:
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_Int64Val{Int64Val: arr.(*array.Int64).Value(idx)}})
			}
		}
	case arrow.PrimitiveTypes.Float32:
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_FloatVal{FloatVal: arr.(*array.Float32).Value(idx)}})
			}
		}
	case arrow.PrimitiveTypes.Float64:
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_DoubleVal{DoubleVal: arr.(*array.Float64).Value(idx)}})
			}
		}
	case arrow.FixedWidthTypes.Boolean:
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_BoolVal{BoolVal: arr.(*array.Boolean).Value(idx)}})
			}
		}
	case arrow.BinaryTypes.Binary:
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_BytesVal{BytesVal: arr.(*array.Binary).Value(idx)}})
			}
		}
	case arrow.BinaryTypes.String:
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_StringVal{StringVal: arr.(*array.String).Value(idx)}})
			}
		}
	case arrow.FixedWidthTypes.Timestamp_s:
		for idx := 0; idx < arr.Len(); idx++ {
			if arr.IsNull(idx) {
				values = append(values, &types.Value{})
			} else {
				values = append(values, &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: int64(arr.(*array.Timestamp).Value(idx))}})
			}
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
			if fieldType != nil {
				break
			}
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

func ArrowValuesToRepeatedProtoValues(arr arrow.Array) ([]*types.RepeatedValue, error) {
	repeatedValues := make([]*types.RepeatedValue, 0, arr.Len())

	if listArr, ok := arr.(*array.List); ok {
		listValues := listArr.ListValues()
		offsets := listArr.Offsets()[1:]
		pos := 0
		for i := 0; i < listArr.Len(); i++ {
			if listArr.IsNull(i) {
				repeatedValues = append(repeatedValues, &types.RepeatedValue{Val: make([]*types.Value, 0)})
				continue
			}

			values := make([]*types.Value, 0, int(offsets[i])-pos)

			for j := pos; j < int(offsets[i]); j++ {
				var protoVal *types.Value

				switch listValues.DataType() {
				case arrow.PrimitiveTypes.Int32:
					protoVal = &types.Value{Val: &types.Value_Int32Val{Int32Val: listValues.(*array.Int32).Value(j)}}
				case arrow.PrimitiveTypes.Int64:
					protoVal = &types.Value{Val: &types.Value_Int64Val{Int64Val: listValues.(*array.Int64).Value(j)}}
				case arrow.PrimitiveTypes.Float32:
					protoVal = &types.Value{Val: &types.Value_FloatVal{FloatVal: listValues.(*array.Float32).Value(j)}}
				case arrow.PrimitiveTypes.Float64:
					protoVal = &types.Value{Val: &types.Value_DoubleVal{DoubleVal: listValues.(*array.Float64).Value(j)}}
				case arrow.BinaryTypes.Binary:
					protoVal = &types.Value{Val: &types.Value_BytesVal{BytesVal: listValues.(*array.Binary).Value(j)}}
				case arrow.BinaryTypes.String:
					protoVal = &types.Value{Val: &types.Value_StringVal{StringVal: listValues.(*array.String).Value(j)}}
				case arrow.FixedWidthTypes.Boolean:
					protoVal = &types.Value{Val: &types.Value_BoolVal{BoolVal: listValues.(*array.Boolean).Value(j)}}
				case arrow.FixedWidthTypes.Timestamp_s:
					protoVal = &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: int64(listValues.(*array.Timestamp).Value(j))}}
				default:
					return nil, fmt.Errorf("unsupported data type in list: %s", listValues.DataType())
				}

				values = append(values, protoVal)
			}

			repeatedValues = append(repeatedValues, &types.RepeatedValue{Val: values})
			// set the end of current element as start of the next
			pos = int(offsets[i])
		}

		return repeatedValues, nil
	}

	protoValues, err := ArrowValuesToProtoValues(arr)
	if err != nil {
		return nil, fmt.Errorf("error converting values to proto Values: %v", err)
	}

	for _, val := range protoValues {
		repeatedValues = append(repeatedValues, &types.RepeatedValue{Val: []*types.Value{val}})
	}

	return repeatedValues, nil
}

func RepeatedProtoValuesToArrowArray(repeatedValues []*types.RepeatedValue, allocator memory.Allocator, numRows int) (arrow.Array, error) {
	if len(repeatedValues) == 0 {
		return array.NewNull(numRows), nil
	}

	var valueType arrow.DataType
	var protoValue *types.Value

	for _, rv := range repeatedValues {
		if rv != nil && len(rv.Val) > 0 {
			for _, val := range rv.Val {
				if val != nil && val.Val != nil {
					protoValue = val
					break
				}
			}
			if protoValue != nil {
				break
			}
		}
	}

	if protoValue == nil {
		return array.NewNull(numRows), nil
	}

	var err error
	valueType, err = ProtoTypeToArrowType(protoValue)
	if err != nil {
		return nil, err
	}

	listBuilder := array.NewListBuilder(allocator, valueType)
	defer listBuilder.Release()
	valueBuilder := listBuilder.ValueBuilder()

	for _, repeatedValue := range repeatedValues {
		listBuilder.Append(true)

		if repeatedValue == nil || len(repeatedValue.Val) == 0 {
			continue
		}

		for _, val := range repeatedValue.Val {
			if val == nil || val.Val == nil {
				appendNullByType(valueBuilder)
				continue
			}

			switch v := val.Val.(type) {
			case *types.Value_Int32Val:
				valueBuilder.(*array.Int32Builder).Append(v.Int32Val)
			case *types.Value_Int64Val:
				valueBuilder.(*array.Int64Builder).Append(v.Int64Val)
			case *types.Value_FloatVal:
				valueBuilder.(*array.Float32Builder).Append(v.FloatVal)
			case *types.Value_DoubleVal:
				valueBuilder.(*array.Float64Builder).Append(v.DoubleVal)
			case *types.Value_BoolVal:
				valueBuilder.(*array.BooleanBuilder).Append(v.BoolVal)
			case *types.Value_StringVal:
				valueBuilder.(*array.StringBuilder).Append(v.StringVal)
			case *types.Value_BytesVal:
				valueBuilder.(*array.BinaryBuilder).Append(v.BytesVal)
			case *types.Value_UnixTimestampVal:
				valueBuilder.(*array.TimestampBuilder).Append(arrow.Timestamp(v.UnixTimestampVal))
			default:
				appendNullByType(valueBuilder)
			}
		}
	}

	for i := len(repeatedValues); i < numRows; i++ {
		listBuilder.Append(true)
	}

	return listBuilder.NewArray(), nil
}

func appendNullByType(builder array.Builder) {
	switch builder.Type().ID() {
	case arrow.INT32:
		builder.(*array.Int32Builder).AppendNull()
	case arrow.INT64:
		builder.(*array.Int64Builder).AppendNull()
	case arrow.FLOAT32:
		builder.(*array.Float32Builder).AppendNull()
	case arrow.FLOAT64:
		builder.(*array.Float64Builder).AppendNull()
	case arrow.BOOL:
		builder.(*array.BooleanBuilder).AppendNull()
	case arrow.STRING:
		builder.(*array.StringBuilder).AppendNull()
	case arrow.BINARY:
		builder.(*array.BinaryBuilder).AppendNull()
	case arrow.TIMESTAMP:
		builder.(*array.TimestampBuilder).AppendNull()
	default:
		builder.AppendNull()
	}
}
