package types

import (
	"encoding/base64"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"time"

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
	if listArr, ok := arr.(*array.List); ok {
		valueList, err := ArrowListToProtoList(listArr, listArr.Offsets())
		if err != nil {
			return nil, fmt.Errorf("error converting list to proto Value: %v", err)
		}
		return valueList, nil
	}

	values := make([]*types.Value, 0)

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

func ArrowListToProtoList(listArr *array.List, inputOffsets []int32) ([]*types.Value, error) {
	listValues := listArr.ListValues()
	offsets := inputOffsets[1:]
	pos := int(inputOffsets[0])
	values := make([]*types.Value, len(offsets))
	for idx := 0; idx < len(offsets); idx++ {
		if listArr.IsValid(idx) {
			value, err := arrowListValuesToProtoValue(listValues, int64(pos), int64(offsets[idx]))
			if err != nil {
				return nil, fmt.Errorf("error converting arrow list to proto Value: %v", err)
			}
			values[idx] = value

		} else {
			values[idx] = &types.Value{}
		}
		pos = int(offsets[idx])
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

// Handles conversion for nested list case for Arrow Array List Builders
func arrowNestedListToRepeatedValues(arrayListVals *array.List, start, end int) ([]*types.Value, error) {
	repeatedValue := make([]*types.Value, 0, end-start)
	for j := start; j < end; j++ {
		valStart, valEnd := arrayListVals.ValueOffsets(j)
		if !arrayListVals.IsValid(j) {
			repeatedValue = append(repeatedValue, &types.Value{})
			continue
		}
		listVals := arrayListVals.ListValues()
		protoVal, err := arrowListValuesToProtoValue(listVals, valStart, valEnd)
		if err != nil {
			return nil, err
		}
		repeatedValue = append(repeatedValue, protoVal)
	}
	return repeatedValue, nil
}

// Handles conversion for primitive type case for Arrow Array List Builders
func arrowPrimitiveListToRepeatedValues(arrayListValues arrow.Array, start int, end int) ([]*types.Value, error) {
	values := make([]*types.Value, 0, end-start)
	for j := start; j < end; j++ {
		var protoVal *types.Value
		if arrayListValues.IsNull(j) {
			protoVal = &types.Value{}
		} else {
			var err error
			protoVal, err = arrowPrimitiveValueToProtoValue(arrayListValues, j)
			if err != nil {
				return nil, err
			}
		}
		values = append(values, protoVal)
	}
	return values, nil
}

// Converts Arrow list values to proto Value for nested lists
func arrowListValuesToProtoValue(listVals arrow.Array, valStart int64, valEnd int64) (*types.Value, error) {
	switch listVals.DataType() {
	case arrow.PrimitiveTypes.Int32:
		vals := make([]int32, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = listVals.(*array.Int32).Value(k + int(valStart))
		}
		return &types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: vals}}}, nil
	case arrow.PrimitiveTypes.Int64:
		vals := make([]int64, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = listVals.(*array.Int64).Value(k + int(valStart))
		}
		return &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: vals}}}, nil
	case arrow.PrimitiveTypes.Float32:
		vals := make([]float32, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = listVals.(*array.Float32).Value(k + int(valStart))
		}
		return &types.Value{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: vals}}}, nil
	case arrow.PrimitiveTypes.Float64:
		vals := make([]float64, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = listVals.(*array.Float64).Value(k + int(valStart))
		}
		return &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: vals}}}, nil
	case arrow.BinaryTypes.Binary:
		vals := make([][]byte, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = listVals.(*array.Binary).Value(k + int(valStart))
		}
		return &types.Value{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: vals}}}, nil
	case arrow.BinaryTypes.String:
		vals := make([]string, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = listVals.(*array.String).Value(k + int(valStart))
		}
		return &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: vals}}}, nil
	case arrow.FixedWidthTypes.Boolean:
		vals := make([]bool, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = listVals.(*array.Boolean).Value(k + int(valStart))
		}
		return &types.Value{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: vals}}}, nil
	case arrow.FixedWidthTypes.Timestamp_s:
		vals := make([]int64, valEnd-valStart)
		for k := 0; k < int(valEnd-valStart); k++ {
			vals[k] = int64(listVals.(*array.Timestamp).Value(k + int(valStart)))
		}
		return &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: vals}}}, nil
	default:
		return nil, fmt.Errorf("unsupported data type in list: %s", listVals.DataType())
	}
}

// Converts Arrow primitive value to proto Value
func arrowPrimitiveValueToProtoValue(arr arrow.Array, idx int) (*types.Value, error) {
	switch arr.DataType() {
	case arrow.PrimitiveTypes.Int32:
		return &types.Value{Val: &types.Value_Int32Val{Int32Val: arr.(*array.Int32).Value(idx)}}, nil
	case arrow.PrimitiveTypes.Int64:
		return &types.Value{Val: &types.Value_Int64Val{Int64Val: arr.(*array.Int64).Value(idx)}}, nil
	case arrow.PrimitiveTypes.Float32:
		return &types.Value{Val: &types.Value_FloatVal{FloatVal: arr.(*array.Float32).Value(idx)}}, nil
	case arrow.PrimitiveTypes.Float64:
		return &types.Value{Val: &types.Value_DoubleVal{DoubleVal: arr.(*array.Float64).Value(idx)}}, nil
	case arrow.BinaryTypes.Binary:
		return &types.Value{Val: &types.Value_BytesVal{BytesVal: arr.(*array.Binary).Value(idx)}}, nil
	case arrow.BinaryTypes.String:
		return &types.Value{Val: &types.Value_StringVal{StringVal: arr.(*array.String).Value(idx)}}, nil
	case arrow.FixedWidthTypes.Boolean:
		return &types.Value{Val: &types.Value_BoolVal{BoolVal: arr.(*array.Boolean).Value(idx)}}, nil
	case arrow.FixedWidthTypes.Timestamp_s:
		return &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: int64(arr.(*array.Timestamp).Value(idx))}}, nil
	case arrow.Null:
		return &types.Value{}, nil
	default:
		return nil, fmt.Errorf("unsupported data type in list: %s", arr.DataType())
	}
}

func ArrowValuesToRepeatedProtoValues(arr arrow.Array) ([]*types.RepeatedValue, error) {
	if arr.Len() == 0 && arr.DataType() == arrow.Null {
		return []*types.RepeatedValue{nil}, nil
	}
	repeatedValues := make([]*types.RepeatedValue, 0, arr.Len())

	arrayList := arr.(*array.List)
	arrayListValues := arrayList.ListValues()

	for i := 0; i < arrayList.Len(); i++ {
		if !arrayList.IsValid(i) {
			repeatedValues = append(repeatedValues, nil)
			continue
		}

		start := int(arrayList.Offsets()[i])
		end := int(arrayList.Offsets()[i+1])

		if arrayListVals, ok := arrayListValues.(*array.List); ok {
			repeatedValue, err := arrowNestedListToRepeatedValues(arrayListVals, start, end)
			if err != nil {
				return nil, err
			}
			repeatedValues = append(repeatedValues, &types.RepeatedValue{Val: repeatedValue})
		} else {
			values, err := arrowPrimitiveListToRepeatedValues(arrayListValues, start, end)
			if err != nil {
				return nil, err
			}
			repeatedValues = append(repeatedValues, &types.RepeatedValue{Val: values})
		}
	}
	return repeatedValues, nil
}

func RepeatedProtoValuesToArrowArray(repeatedValues []*types.RepeatedValue, allocator memory.Allocator) (arrow.Array, error) {
	var valueType arrow.DataType
	var protoValue *types.Value
	var err error

	// Find the first non-nil proto value in repeatedValues
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

	if protoValue != nil {
		// Determine the value type from the first non-nil proto value
		valueType, err = ProtoTypeToArrowType(protoValue)
		if err != nil {
			return nil, err
		}

		listBuilder := array.NewListBuilder(allocator, valueType)
		defer listBuilder.Release()
		valueBuilder := listBuilder.ValueBuilder()

		for _, repeatedValue := range repeatedValues {
			if repeatedValue == nil {
				listBuilder.AppendNull()
				continue
			}

			if len(repeatedValue.Val) == 0 && repeatedValue.Val == nil {
				return nil, fmt.Errorf("represent it as an empty array instead of nil")
			}
			listBuilder.Append(true)

			err = CopyProtoValuesToArrowArray(valueBuilder, repeatedValue.Val)
			if err != nil {
				return nil, fmt.Errorf("error copying proto values to arrow array: %v", err)
			}
		}
		return listBuilder.NewArray(), nil

	} else {
		if len(repeatedValues) == 0 {
			return array.NewNull(0), nil
		} else {
			nullListBuilder := array.NewListBuilder(allocator, arrow.Null)
			defer nullListBuilder.Release()
			nullValueBuilder := nullListBuilder.ValueBuilder()
			for _, repeatedVal := range repeatedValues {

				if repeatedVal == nil {
					nullListBuilder.AppendNull()
					continue
				}

				if len(repeatedVal.Val) == 0 && repeatedVal.Val == nil {
					return nil, fmt.Errorf("represent it as an empty array instead of nil")
				}

				nullListBuilder.Append(true)
				for range repeatedVal.Val {
					nullValueBuilder.AppendNull()
				}
			}
			return nullListBuilder.NewArray(), nil
		}
	}
}

func InterfaceToProtoValue(val interface{}) (*types.Value, error) {
	protoVal := &types.Value{}

	if val == nil {
		return protoVal, nil
	}

	switch v := val.(type) {
	case []byte:
		protoVal.Val = &types.Value_BytesVal{BytesVal: v}
	case string:
		protoVal.Val = &types.Value_StringVal{StringVal: v}
	case int32:
		protoVal.Val = &types.Value_Int32Val{Int32Val: v}
	case int64:
		protoVal.Val = &types.Value_Int64Val{Int64Val: v}
	case float64:
		protoVal.Val = &types.Value_DoubleVal{DoubleVal: v}
	case float32:
		protoVal.Val = &types.Value_FloatVal{FloatVal: v}
	case bool:
		protoVal.Val = &types.Value_BoolVal{BoolVal: v}
	case time.Time:
		protoVal.Val = &types.Value_UnixTimestampVal{UnixTimestampVal: v.Unix()}
	case *timestamppb.Timestamp:
		protoVal.Val = &types.Value_UnixTimestampVal{UnixTimestampVal: GetTimestampSeconds(v)}

	case [][]byte:
		bytesList := &types.BytesList{Val: v}
		protoVal.Val = &types.Value_BytesListVal{BytesListVal: bytesList}
	case types.BytesList:
		protoVal.Val = &types.Value_BytesListVal{BytesListVal: &v}
	case *types.BytesList:
		protoVal.Val = &types.Value_BytesListVal{BytesListVal: v}

	case []string:
		stringList := &types.StringList{Val: v}
		protoVal.Val = &types.Value_StringListVal{StringListVal: stringList}
	case types.StringList:
		protoVal.Val = &types.Value_StringListVal{StringListVal: &v}
	case *types.StringList:
		protoVal.Val = &types.Value_StringListVal{StringListVal: v}

	case []int:
		intList := make([]int32, len(v))
		for i, num := range v {
			intList[i] = int32(num)
		}
		int32List := &types.Int32List{Val: intList}
		protoVal.Val = &types.Value_Int32ListVal{Int32ListVal: int32List}
	case []int32:
		int32List := &types.Int32List{Val: v}
		protoVal.Val = &types.Value_Int32ListVal{Int32ListVal: int32List}
	case types.Int32List:
		protoVal.Val = &types.Value_Int32ListVal{Int32ListVal: &v}
	case *types.Int32List:
		protoVal.Val = &types.Value_Int32ListVal{Int32ListVal: v}

	case []int64:
		int64List := &types.Int64List{Val: v}
		protoVal.Val = &types.Value_Int64ListVal{Int64ListVal: int64List}
	case types.Int64List:
		protoVal.Val = &types.Value_Int64ListVal{Int64ListVal: &v}
	case *types.Int64List:
		protoVal.Val = &types.Value_Int64ListVal{Int64ListVal: v}

	case []float64:
		doubleList := &types.DoubleList{Val: v}
		protoVal.Val = &types.Value_DoubleListVal{DoubleListVal: doubleList}
	case types.DoubleList:
		protoVal.Val = &types.Value_DoubleListVal{DoubleListVal: &v}
	case *types.DoubleList:
		protoVal.Val = &types.Value_DoubleListVal{DoubleListVal: v}

	case []float32:
		floatList := &types.FloatList{Val: v}
		protoVal.Val = &types.Value_FloatListVal{FloatListVal: floatList}
	case types.FloatList:
		protoVal.Val = &types.Value_FloatListVal{FloatListVal: &v}
	case *types.FloatList:
		protoVal.Val = &types.Value_FloatListVal{FloatListVal: v}

	case []bool:
		boolList := &types.BoolList{Val: v}
		protoVal.Val = &types.Value_BoolListVal{BoolListVal: boolList}
	case types.BoolList:
		protoVal.Val = &types.Value_BoolListVal{BoolListVal: &v}
	case *types.BoolList:
		protoVal.Val = &types.Value_BoolListVal{BoolListVal: v}

	case []time.Time:
		timestamps := make([]int64, len(v))
		for j, t := range v {
			timestamps[j] = t.Unix()
		}
		timestampList := &types.Int64List{Val: timestamps}
		protoVal.Val = &types.Value_UnixTimestampListVal{UnixTimestampListVal: timestampList}

	case []*timestamppb.Timestamp:
		timestamps := make([]int64, len(v))
		for j, t := range v {
			timestamps[j] = GetTimestampSeconds(t)
		}
		timestampList := &types.Int64List{Val: timestamps}
		protoVal.Val = &types.Value_UnixTimestampListVal{UnixTimestampListVal: timestampList}

	case types.Null:
		protoVal.Val = &types.Value_NullVal{NullVal: types.Null_NULL}

	case *types.Value:
		protoVal = v

	default:
		if tryConvertToInt32(&protoVal, v) ||
			tryConvertToInt64(&protoVal, v) ||
			tryConvertToFloat(&protoVal, v) ||
			tryConvertToDouble(&protoVal, v) {
			return protoVal, nil
		}
		return nil, fmt.Errorf("unsupported value type: %T", v)
	}

	return protoVal, nil
}

func tryConvertToInt32(protoVal **types.Value, v interface{}) bool {
	switch num := v.(type) {
	case int:
		if num <= math.MaxInt32 && num >= math.MinInt32 {
			(*protoVal).Val = &types.Value_Int32Val{Int32Val: int32(num)}
			return true
		}
	case int8:
		(*protoVal).Val = &types.Value_Int32Val{Int32Val: int32(num)}
		return true
	case int16:
		(*protoVal).Val = &types.Value_Int32Val{Int32Val: int32(num)}
		return true
	case uint8:
		(*protoVal).Val = &types.Value_Int32Val{Int32Val: int32(num)}
		return true
	case uint16:
		(*protoVal).Val = &types.Value_Int32Val{Int32Val: int32(num)}
		return true
	case uint:
		if num <= math.MaxInt32 {
			(*protoVal).Val = &types.Value_Int32Val{Int32Val: int32(num)}
			return true
		}
	}
	return false
}

func tryConvertToInt64(protoVal **types.Value, v interface{}) bool {
	switch num := v.(type) {
	case int:
		(*protoVal).Val = &types.Value_Int64Val{Int64Val: int64(num)}
		return true
	case uint:
		if num <= math.MaxInt64 {
			(*protoVal).Val = &types.Value_Int64Val{Int64Val: int64(num)}
			return true
		}
	case uint32:
		(*protoVal).Val = &types.Value_Int64Val{Int64Val: int64(num)}
		return true
	case uint64:
		if num <= math.MaxInt64 {
			(*protoVal).Val = &types.Value_Int64Val{Int64Val: int64(num)}
			return true
		}
	}
	return false
}

func tryConvertToFloat(protoVal **types.Value, v interface{}) bool {
	switch num := v.(type) {
	case float32:
		(*protoVal).Val = &types.Value_FloatVal{FloatVal: num}
		return true
	}
	return false
}

func tryConvertToDouble(protoVal **types.Value, v interface{}) bool {
	switch num := v.(type) {
	case float64:
		(*protoVal).Val = &types.Value_DoubleVal{DoubleVal: num}
		return true
	}
	return false
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

func ValueTypeToGoType(value *types.Value) interface{} {
	return valueTypeToGoTypeTimestampAsString(value, false)
}

func ValueTypeToGoTypeTimestampAsString(value *types.Value) interface{} {
	return valueTypeToGoTypeTimestampAsString(value, true)
}

var TimestampFormat = "2006-01-02 15:04:05.999999999Z0700"

func valueTypeToGoTypeTimestampAsString(value *types.Value, timestampAsString bool) interface{} {
	if value == nil || value.Val == nil {
		return nil
	}

	switch x := value.Val.(type) {
	case *types.Value_StringVal:
		return x.StringVal
	case *types.Value_BytesVal:
		return x.BytesVal
	case *types.Value_Int32Val:
		return x.Int32Val
	case *types.Value_Int64Val:
		return x.Int64Val
	case *types.Value_FloatVal:
		return x.FloatVal
	case *types.Value_DoubleVal:
		return x.DoubleVal
	case *types.Value_BoolVal:
		return x.BoolVal
	case *types.Value_BoolListVal:
		return x.BoolListVal.Val
	case *types.Value_StringListVal:
		return x.StringListVal.Val
	case *types.Value_BytesListVal:
		return x.BytesListVal.Val
	case *types.Value_Int32ListVal:
		return x.Int32ListVal.Val
	case *types.Value_Int64ListVal:
		return x.Int64ListVal.Val
	case *types.Value_FloatListVal:
		return x.FloatListVal.Val
	case *types.Value_DoubleListVal:
		return x.DoubleListVal.Val
	case *types.Value_UnixTimestampVal:
		if timestampAsString {
			return time.Unix(x.UnixTimestampVal, 0).UTC().Format(TimestampFormat)
		}
		return time.Unix(x.UnixTimestampVal, 0).UTC()
	case *types.Value_UnixTimestampListVal:
		if timestampAsString {
			timestamps := make([]string, len(x.UnixTimestampListVal.Val))
			for i, ts := range x.UnixTimestampListVal.Val {
				timestamps[i] = time.Unix(ts, 0).UTC().Format(TimestampFormat)
			}
			return timestamps
		}
		timestamps := make([]time.Time, len(x.UnixTimestampListVal.Val))
		for i, ts := range x.UnixTimestampListVal.Val {
			timestamps[i] = time.Unix(ts, 0).UTC()
		}
		return timestamps
	default:
		return nil
	}
}

func transformStringToBytes(str string) []byte {
	bytes, decodeErr := base64.StdEncoding.DecodeString(str)
	if decodeErr != nil {
		// If base64 decoding fails, try as a byte string
		bytes = []byte(str)
	}
	return bytes
}

func ConvertToValueType(value *types.Value, valueType types.ValueType_Enum) (*types.Value, error) {
	if valueType != types.ValueType_NULL {
		if value == nil || value.Val == nil {
			return nil, fmt.Errorf("value is nil, cannot convert to type %s", valueType)
		}
		switch value.Val.(type) {
		case *types.Value_NullVal:
			return nil, fmt.Errorf("value is nil, cannot convert to type %s", valueType)
		}
	}

	err := fmt.Errorf("unsupported value type for conversion: %s for actual value type: %T", valueType, value.GetVal())

	switch valueType {
	case types.ValueType_STRING:
		switch value.Val.(type) {
		case *types.Value_StringVal:
			return value, nil
		}
	case types.ValueType_BYTES:
		switch value.Val.(type) {
		case *types.Value_BytesVal:
			return value, nil
		case *types.Value_StringVal:
			return &types.Value{Val: &types.Value_BytesVal{BytesVal: transformStringToBytes(value.GetStringVal())}}, nil
		}
	case types.ValueType_INT32:
		switch value.Val.(type) {
		case *types.Value_Int32Val:
			return value, nil
		case *types.Value_Int64Val:
			if value.GetInt64Val() < math.MinInt32 || value.GetInt64Val() > math.MaxInt32 {
				return nil, fmt.Errorf("value %d is out of range for %s", value.GetInt64Val(), valueType)
			}
			return &types.Value{Val: &types.Value_Int32Val{Int32Val: int32(value.GetInt64Val())}}, nil
		}
	case types.ValueType_INT64:
		switch value.Val.(type) {
		case *types.Value_Int32Val:
			return &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(value.GetInt32Val())}}, nil
		case *types.Value_Int64Val:
			return value, nil
		}
	case types.ValueType_FLOAT:
		switch value.Val.(type) {
		case *types.Value_FloatVal:
			return value, nil
		case *types.Value_DoubleVal:
			if value.GetDoubleVal() < math.SmallestNonzeroFloat32 || value.GetDoubleVal() > math.MaxFloat32 {
				return nil, fmt.Errorf("value %e is out of range for %s", value.GetDoubleVal(), valueType)
			}
			return &types.Value{Val: &types.Value_FloatVal{FloatVal: float32(value.GetDoubleVal())}}, nil
		}
	case types.ValueType_DOUBLE:
		switch value.Val.(type) {
		case *types.Value_FloatVal:
			return &types.Value{Val: &types.Value_DoubleVal{DoubleVal: float64(value.GetFloatVal())}}, nil
		case *types.Value_DoubleVal:
			return value, nil
		}
	case types.ValueType_UNIX_TIMESTAMP:
		switch value.Val.(type) {
		case *types.Value_UnixTimestampVal:
			return value, nil
		case *types.Value_Int32Val:
			return &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: int64(value.GetInt32Val())}}, nil
		case *types.Value_Int64Val:
			return &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: value.GetInt64Val()}}, nil
		}
	case types.ValueType_BOOL:
		switch value.Val.(type) {
		case *types.Value_BoolVal:
			return value, nil
		}
	case types.ValueType_STRING_LIST:
		switch value.Val.(type) {
		case *types.Value_StringListVal:
			return value, nil
		}
	case types.ValueType_BYTES_LIST:
		switch value.Val.(type) {
		case *types.Value_BytesListVal:
			return value, nil
		case *types.Value_StringListVal:
			stringList := value.GetStringListVal().GetVal()
			bytesList := make([][]byte, len(stringList))
			for i, str := range stringList {
				bytesList[i] = transformStringToBytes(str)
			}
			return &types.Value{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: bytesList}}}, nil
		}
	case types.ValueType_INT32_LIST:
		switch value.Val.(type) {
		case *types.Value_Int32ListVal:
			return value, nil
		case *types.Value_Int64ListVal:
			int64List := value.GetInt64ListVal().GetVal()
			int32List := make([]int32, len(int64List))
			for i, v := range int64List {
				if v < math.MinInt32 || v > math.MaxInt32 {
					return nil, fmt.Errorf("value %d is out of range for %s", v, valueType)
				}
				int32List[i] = int32(v)
			}
			return &types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: int32List}}}, nil
		}
	case types.ValueType_INT64_LIST:
		switch value.Val.(type) {
		case *types.Value_Int32ListVal:
			int32List := value.GetInt32ListVal().GetVal()
			int64List := make([]int64, len(int32List))
			for i, v := range int32List {
				int64List[i] = int64(v)
			}
			return &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: int64List}}}, nil
		case *types.Value_Int64ListVal:
			return value, nil
		}
	case types.ValueType_FLOAT_LIST:
		switch value.Val.(type) {
		case *types.Value_FloatListVal:
			return value, nil
		case *types.Value_DoubleListVal:
			doubleList := value.GetDoubleListVal().GetVal()
			floatList := make([]float32, len(doubleList))
			for i, v := range doubleList {
				if v < math.SmallestNonzeroFloat32 || v > math.MaxFloat32 {
					return nil, fmt.Errorf("value %e is out of range for %s", v, valueType)
				}
				floatList[i] = float32(v)
			}
			return &types.Value{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: floatList}}}, nil
		}
	case types.ValueType_DOUBLE_LIST:
		switch value.Val.(type) {
		case *types.Value_FloatListVal:
			floatList := value.GetFloatListVal().GetVal()
			doubleList := make([]float64, len(floatList))
			for i, v := range floatList {
				doubleList[i] = float64(v)
			}
			return &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: doubleList}}}, nil
		case *types.Value_DoubleListVal:
			return value, nil
		}
	case types.ValueType_UNIX_TIMESTAMP_LIST:
		switch value.Val.(type) {
		case *types.Value_UnixTimestampListVal:
			return value, nil
		case *types.Value_Int32ListVal:
			int32List := value.GetInt32ListVal().GetVal()
			unixTimestampList := make([]int64, len(int32List))
			for i, v := range int32List {
				unixTimestampList[i] = int64(v)
			}
			return &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: unixTimestampList}}}, nil
		case *types.Value_Int64ListVal:
			return &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: value.GetInt64ListVal().GetVal()}}}, nil
		}
	case types.ValueType_BOOL_LIST:
		switch value.Val.(type) {
		case *types.Value_BoolListVal:
			return value, nil
		}
	case types.ValueType_NULL:
		if value == nil || value.Val == nil {
			return nil, nil
		}
		switch value.Val.(type) {
		case *types.Value_NullVal:
			return nil, nil
		default:
			return nil, fmt.Errorf("value is not null, cannot convert type %T", value.Val)
		}
	}

	return nil, err
}

func GetTimestampSeconds(ts *timestamppb.Timestamp) int64 {
	if ts == nil {
		return 0
	}
	return ts.GetSeconds()
}
