//go:build !integration

package types

import (
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/feast-dev/feast/go/protos/feast/types"
)

var nil_or_null_val = &types.Value{}

var (
	PROTO_VALUES = [][]*types.Value{
		{{Val: nil}},
		{{Val: nil}, {Val: nil}},
		{nil_or_null_val, nil_or_null_val},
		{nil_or_null_val, {Val: nil}},
		{{Val: &types.Value_Int32Val{10}}, {Val: nil}, nil_or_null_val, {Val: &types.Value_Int32Val{20}}},
		{{Val: &types.Value_Int32Val{10}}, nil_or_null_val},
		{nil_or_null_val, {Val: &types.Value_Int32Val{20}}},
		{{Val: &types.Value_Int32Val{10}}, {Val: &types.Value_Int32Val{20}}},
		{{Val: &types.Value_Int64Val{10}}, nil_or_null_val},
		{{Val: &types.Value_Int64Val{10}}, {Val: &types.Value_Int64Val{20}}},
		{nil_or_null_val, {Val: &types.Value_FloatVal{2.0}}},
		{{Val: &types.Value_FloatVal{1.0}}, {Val: &types.Value_FloatVal{2.0}}},
		{{Val: &types.Value_FloatVal{1.0}}, {Val: &types.Value_FloatVal{2.0}}, {Val: &types.Value_FloatVal{float32(math.NaN())}}},
		{{Val: &types.Value_DoubleVal{1.0}}, {Val: &types.Value_DoubleVal{2.0}}},
		{{Val: &types.Value_DoubleVal{1.0}}, {Val: &types.Value_DoubleVal{2.0}}, {Val: &types.Value_DoubleVal{math.NaN()}}},
		{{Val: &types.Value_DoubleVal{1.0}}, nil_or_null_val},
		{nil_or_null_val, {Val: &types.Value_StringVal{"bbb"}}},
		{{Val: &types.Value_StringVal{"aaa"}}, {Val: &types.Value_StringVal{"bbb"}}},
		{{Val: &types.Value_BytesVal{[]byte{1, 2, 3}}}, nil_or_null_val},
		{{Val: &types.Value_BytesVal{[]byte{1, 2, 3}}}, {Val: &types.Value_BytesVal{[]byte{4, 5, 6}}}},
		{nil_or_null_val, {Val: &types.Value_BoolVal{false}}},
		{{Val: &types.Value_BoolVal{true}}, {Val: &types.Value_BoolVal{false}}},
		{{Val: &types.Value_UnixTimestampVal{time.Now().Unix()}}, nil_or_null_val},
		{{Val: &types.Value_UnixTimestampVal{time.Now().Unix()}}, {Val: &types.Value_UnixTimestampVal{time.Now().Unix()}}},
		{{Val: &types.Value_UnixTimestampVal{time.Now().Unix()}}, {Val: &types.Value_UnixTimestampVal{time.Now().Unix()}}, {Val: &types.Value_UnixTimestampVal{-9223372036854775808}}},

		{
			{Val: &types.Value_Int32ListVal{&types.Int32List{Val: []int32{0, 1, 2}}}},
			{Val: &types.Value_Int32ListVal{&types.Int32List{Val: []int32{3, 4, 5}}}},
			{Val: &types.Value_Int32ListVal{&types.Int32List{Val: []int32{}}}},
			{},
		},
		{
			{Val: &types.Value_Int64ListVal{&types.Int64List{Val: []int64{0, 1, 2, 553248634761893728}}}},
			{Val: &types.Value_Int64ListVal{&types.Int64List{Val: []int64{3, 4, 5, 553248634761893729}}}},
		},
		{
			{Val: &types.Value_FloatListVal{&types.FloatList{Val: []float32{0.5, 1.5, 2}}}},
			{Val: &types.Value_FloatListVal{&types.FloatList{Val: []float32{3.5, 4, 5}}}},
		},
		{
			{Val: &types.Value_DoubleListVal{&types.DoubleList{Val: []float64{0.5, 1, 2}}}},
			{Val: &types.Value_DoubleListVal{&types.DoubleList{Val: []float64{3.5, 4, 5}}}},
		},
		{
			{Val: &types.Value_BytesListVal{&types.BytesList{Val: [][]byte{{0, 1}, {2}}}}},
			{Val: &types.Value_BytesListVal{&types.BytesList{Val: [][]byte{{3, 4}, {5}}}}},
		},
		{
			{Val: &types.Value_StringListVal{&types.StringList{Val: []string{"aa", "bb"}}}},
			{Val: &types.Value_StringListVal{&types.StringList{Val: []string{"cc", "dd"}}}},
		},
		{
			{Val: &types.Value_BoolListVal{&types.BoolList{Val: []bool{false, false}}}},
			{Val: &types.Value_BoolListVal{&types.BoolList{Val: []bool{true, true}}}},
		},
		{
			{Val: &types.Value_UnixTimestampListVal{&types.Int64List{Val: []int64{time.Now().Unix()}}}},
			{Val: &types.Value_UnixTimestampListVal{&types.Int64List{Val: []int64{time.Now().Unix()}}}},
		},
		{
			{Val: &types.Value_UnixTimestampListVal{&types.Int64List{Val: []int64{time.Now().Unix(), time.Now().Unix()}}}},
			{Val: &types.Value_UnixTimestampListVal{&types.Int64List{Val: []int64{time.Now().Unix(), time.Now().Unix()}}}},
			{Val: &types.Value_UnixTimestampListVal{&types.Int64List{Val: []int64{-9223372036854775808, time.Now().Unix()}}}},
		},
	}
)

var (
	REPEATED_PROTO_VALUES = []*types.RepeatedValue{
		nil,
		{Val: []*types.Value{}},
		{Val: []*types.Value{nil_or_null_val}},
		{Val: []*types.Value{nil_or_null_val, nil_or_null_val}},
		{Val: []*types.Value{{}, {}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, {Val: &types.Value_Int32Val{Int32Val: 20}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, nil_or_null_val}},
		{Val: []*types.Value{nil_or_null_val, {Val: &types.Value_Int32Val{Int32Val: 20}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, nil_or_null_val, {Val: &types.Value_Int32Val{Int32Val: 30}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{20, 21}}}}, {Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{30, 31}}}}, {Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{}}}}, {Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{}}}, {}}},
		{Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 10}}}},
		{Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 10}}, {Val: &types.Value_Int64Val{Int64Val: 20}}}},
		{Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 10}}, nil_or_null_val}},
		{Val: []*types.Value{nil_or_null_val, {Val: &types.Value_Int64Val{Int64Val: 20}}}},
		{Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 9223372036854775807}}, {Val: &types.Value_Int64Val{Int64Val: -9223372036854775808}}}},
		{Val: []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 1.0}}}},
		{Val: []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 1.0}}, {Val: &types.Value_FloatVal{FloatVal: 2.0}}}},
		{Val: []*types.Value{nil_or_null_val, {Val: &types.Value_FloatVal{FloatVal: 2.0}}}},
		{Val: []*types.Value{{Val: &types.Value_FloatVal{FloatVal: 1.0}}, {Val: &types.Value_FloatVal{FloatVal: 2.0}}, {Val: &types.Value_FloatVal{FloatVal: float32(math.NaN())}}}},
		{Val: []*types.Value{{Val: &types.Value_DoubleVal{DoubleVal: 1.0}}}},
		{Val: []*types.Value{{Val: &types.Value_DoubleVal{DoubleVal: 1.0}}, {Val: &types.Value_DoubleVal{DoubleVal: 2.0}}}},
		{Val: []*types.Value{{Val: &types.Value_DoubleVal{DoubleVal: 1.0}}, nil_or_null_val}},
		{Val: []*types.Value{{Val: &types.Value_DoubleVal{DoubleVal: 1.0}}, {Val: &types.Value_DoubleVal{DoubleVal: 2.0}}, {Val: &types.Value_DoubleVal{DoubleVal: math.NaN()}}}},
		{Val: []*types.Value{{Val: &types.Value_StringVal{StringVal: "aaa"}}}},
		{Val: []*types.Value{{Val: &types.Value_StringVal{StringVal: "aaa"}}, {Val: &types.Value_StringVal{StringVal: "bbb"}}}},
		{Val: []*types.Value{nil_or_null_val, {Val: &types.Value_StringVal{StringVal: "bbb"}}}},
		{Val: []*types.Value{{Val: &types.Value_StringVal{StringVal: ""}}, {Val: &types.Value_StringVal{StringVal: "non-empty"}}}},
		{Val: []*types.Value{{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}}}},
		{Val: []*types.Value{{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}}, {Val: &types.Value_BytesVal{BytesVal: []byte{4, 5, 6}}}}},
		{Val: []*types.Value{{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}}, nil_or_null_val}},
		{Val: []*types.Value{{Val: &types.Value_BytesVal{BytesVal: []byte{}}}, {Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}}}},
		{Val: []*types.Value{{Val: &types.Value_BoolVal{BoolVal: true}}}},
		{Val: []*types.Value{{Val: &types.Value_BoolVal{BoolVal: true}}, {Val: &types.Value_BoolVal{BoolVal: false}}}},
		{Val: []*types.Value{nil_or_null_val, {Val: &types.Value_BoolVal{BoolVal: false}}}},
		{Val: []*types.Value{{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix()}}}},
		{Val: []*types.Value{{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix()}}, {Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix() + 3600}}}},
		{Val: []*types.Value{{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix()}}, nil_or_null_val}},
		{Val: []*types.Value{{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix()}}, {Val: &types.Value_UnixTimestampVal{UnixTimestampVal: -9223372036854775808}}}},
	}
)

var (
	MULTIPLE_REPEATED_PROTO_VALUES = [][]*types.RepeatedValue{
		{
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, {Val: &types.Value_Int32Val{}}, {}}},
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 20}}}},
			{Val: []*types.Value{}}, // Empty Array
			nil,                     // NULL or Not Found Values
		},
		{
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, {Val: &types.Value_Int32Val{Int32Val: 20}}}},
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 30}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 100}}}},
			{Val: []*types.Value{}},
			{Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 300}}}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_StringVal{StringVal: "row1"}}}},
			{Val: []*types.Value{{Val: &types.Value_StringVal{StringVal: "row2"}}}},
		},
		{
			{Val: []*types.Value{
				{Val: &types.Value_FloatVal{FloatVal: 1.1}},
				{Val: &types.Value_FloatVal{FloatVal: 2.2}},
			}},
			{Val: []*types.Value{
				{Val: &types.Value_FloatVal{FloatVal: 3.3}},
				{Val: &types.Value_FloatVal{FloatVal: 4.4}},
			}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_BoolVal{BoolVal: true}}}},
			{Val: []*types.Value{}},
			{Val: []*types.Value{{Val: &types.Value_BoolVal{BoolVal: false}}}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix()}}}},
			{Val: []*types.Value{{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix() + 3600}}}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}}}},
			{Val: []*types.Value{{Val: &types.Value_BytesVal{BytesVal: []byte{4, 5, 6}}}}},
		},
		{
			{Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 10}},
				{Val: &types.Value_Int32Val{Int32Val: 30}},
			}},
			{Val: []*types.Value{}},
			{Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 40}},
				{Val: &types.Value_Int32Val{Int32Val: 50}},
			}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{10, 11}}}}}},
			{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{20, 21}}}}}},
			{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{}}}}}},
			{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{}}}}},
			{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{10, 11}}}}}},
			{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{20, 30}}}}}},
			nil,
			{Val: []*types.Value{}},
			{Val: []*types.Value{{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{20, 21}}}}, {Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{30, 31}}}}, {Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{}}}}, {}}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{100, 101}}}}}},
			{Val: []*types.Value{{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{200, 201}}}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: []float32{1.1, 1.2}}}}}},
			{Val: []*types.Value{{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: []float32{2.1, 2.2}}}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{1.1, 1.2}}}}}},
			{Val: []*types.Value{{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{2.1, 2.2}}}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: [][]byte{{1, 2}, {3, 4}}}}}}},
			{Val: []*types.Value{{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: [][]byte{{5, 6}, {7, 8}}}}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"row1", "row2"}}}}}},
			{Val: []*types.Value{{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"row3", "row4"}}}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: []bool{true, false}}}}}},
			{Val: []*types.Value{{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: []bool{false, true}}}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{time.Now().Unix()}}}}}},
			{Val: []*types.Value{{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{time.Now().Unix() + 3600}}}}}},
			{Val: []*types.Value{}},
		},
		{
			{Val: []*types.Value{}},
			{Val: []*types.Value{}},
			{Val: []*types.Value{}},
			{Val: []*types.Value{}},
		},
	}
)

func TestConversionBetweenProtoAndArrow(t *testing.T) {
	pool := memory.NewGoAllocator()
	for _, vector := range PROTO_VALUES {
		arrowArray, err := ProtoValuesToArrowArray(vector, pool, len(vector))
		assert.Nil(t, err)

		protoValues, err := ArrowValuesToProtoValues(arrowArray)
		assert.Nil(t, err)

		protoValuesEquals(t, vector, protoValues)
	}

}

func protoValuesEquals(t *testing.T, a, b []*types.Value) {
	assert.Equal(t, len(a), len(b))

	for idx, left := range a {
		assert.Truef(t, proto.Equal(left, b[idx]),
			"Arrays are not equal. Diff[%d] %v != %v", idx, left, b[idx])
	}
}

func protoRepeatedValueEquals(t *testing.T, a *types.RepeatedValue, b *types.RepeatedValue, index int) {
	assert.Truef(t, proto.Equal(a, b),
		"Values are not equal for testcase[%d]. Diff %v != %v", index, a, b)
}

func protoRepeatedValuesEquals(t *testing.T, a, b []*types.RepeatedValue, index int) {
	assert.Equal(t, len(a), len(b))

	for idx, left := range a {
		assert.Truef(t, proto.Equal(left, b[idx]),
			"Arrays are not equal for testcase[%d]. Diff[%d] %v != %v", index, idx, left, b[idx])
	}
}

func TestRepeatedValueRoundTrip(t *testing.T) {
	pool := memory.NewGoAllocator()

	for i, repeatedValue := range REPEATED_PROTO_VALUES {
		arrowArray, err := RepeatedProtoValuesToArrowArray([]*types.RepeatedValue{repeatedValue}, pool)
		assert.Nil(t, err, "Error creating Arrow array for case %d", i)

		result, err := ArrowValuesToRepeatedProtoValues(arrowArray)
		assert.Nil(t, err, "Error converting back for case %d", i)

		assert.Equal(t, 1, len(result), "Should have 1 result for case %d", i)

		protoRepeatedValueEquals(t, repeatedValue, result[0], i)
	}
}

func TestMultipleRepeatedValueRoundTrip(t *testing.T) {
	pool := memory.NewGoAllocator()

	for i, batch := range MULTIPLE_REPEATED_PROTO_VALUES {
		arrowArray, err := RepeatedProtoValuesToArrowArray(batch, pool)
		assert.Nil(t, err, "Error creating Arrow array for batch %d", i)

		results, err := ArrowValuesToRepeatedProtoValues(arrowArray)
		assert.Nil(t, err, "Error converting back for batch %d", i)

		assert.Equal(t, len(batch), len(results),
			"Row count mismatch for batch %d", i)

		protoRepeatedValuesEquals(t, batch, results, i)
	}
}

func TestEmptyAndNullRepeatedValues(t *testing.T) {
	pool := memory.NewGoAllocator()

	testCases := [][]*types.RepeatedValue{
		{{Val: []*types.Value{}}},
		{{Val: []*types.Value{}}, {Val: []*types.Value{}}},
		{{Val: []*types.Value{nil_or_null_val}}},
		{{Val: []*types.Value{nil_or_null_val, nil_or_null_val}}},
		{{Val: []*types.Value{}}, {Val: []*types.Value{nil_or_null_val}}},
	}

	for i, testCase := range testCases {
		arrowArray, err := RepeatedProtoValuesToArrowArray(testCase, pool)
		assert.Nil(t, err, "Error creating Arrow array for case %d", i)

		result, err := ArrowValuesToRepeatedProtoValues(arrowArray)
		assert.Nil(t, err, "Error converting back for case %d", i)

		assert.Equal(t, len(testCase), len(result),
			"Row count mismatch for case %d", i)

		for j := 0; j < len(testCase); j++ {
			if j < len(result) {
				if len(testCase[j].Val) == 0 {
					if len(result[j].Val) == 1 {
						if result[j].Val[0] != nil && result[j].Val[0].Val != nil {
							t.Errorf("Case %d, row %d: Expected empty value or null, got non-null value", i, j)
						}
					} else if len(result[j].Val) > 1 {
						t.Errorf("Case %d, row %d: Expected empty or single null, got %d values",
							i, j, len(result[j].Val))
					}
				}
			}
		}
	}
}

func TestInterfaceToProtoValue(t *testing.T) {
	testTime := time.Now()
	testCases := []struct {
		input    interface{}
		expected *types.Value
	}{
		{nil, &types.Value{}},
		{[]byte{1, 2, 3}, &types.Value{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}}},
		{"test", &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}},
		{int32(10), &types.Value{Val: &types.Value_Int32Val{Int32Val: 10}}},
		{int64(20), &types.Value{Val: &types.Value_Int64Val{Int64Val: 20}}},
		{float32(30.5), &types.Value{Val: &types.Value_FloatVal{FloatVal: 30.5}}},
		{float64(40.5), &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 40.5}}},
		{true, &types.Value{Val: &types.Value_BoolVal{BoolVal: true}}},
		{testTime, &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: testTime.Unix()}}},
		{&timestamppb.Timestamp{Seconds: testTime.Unix(), Nanos: int32(testTime.Nanosecond())}, &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: testTime.Unix()}}},
		{[][]byte{{1, 2}, {3, 4}}, &types.Value{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: [][]byte{{1, 2}, {3, 4}}}}}},
		{[]string{"a", "b"}, &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"a", "b"}}}}},
		{[]int{1, 2}, &types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{1, 2}}}}},
		{[]int32{1, 2}, &types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{1, 2}}}}},
		{[]int64{3, 4}, &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{3, 4}}}}},
		{[]float32{5.5, 6.6}, &types.Value{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: []float32{5.5, 6.6}}}}},
		{[]float64{7.7, 8.8}, &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{7.7, 8.8}}}}},
		{[]bool{true, false}, &types.Value{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: []bool{true, false}}}}},
		{[]time.Time{testTime, testTime.Add(time.Hour)}, &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{testTime.Unix(), testTime.Add(time.Hour).Unix()}}}}},
		{[]*timestamppb.Timestamp{{Seconds: testTime.Unix(), Nanos: int32(testTime.Nanosecond())}, {Seconds: testTime.Add(time.Hour).Unix(), Nanos: int32(testTime.Add(time.Hour).Nanosecond())}}, &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{testTime.Unix(), testTime.Add(time.Hour).Unix()}}}}},
		{&types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}, &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}},
		{&types.Value{Val: &types.Value_StringVal{StringVal: "test"}}, &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}},
	}

	for _, tc := range testCases {
		result, err := InterfaceToProtoValue(tc.input)
		assert.NoError(t, err, "Error converting input %v to proto value", tc.input)
		assert.True(t, proto.Equal(result, tc.expected),
			"Expected %v but got %v for input %v", tc.expected, result, tc.input)
	}
}

func TestValueTypeToGoType(t *testing.T) {
	timestamp := time.Unix(1744769099, 0).UTC()
	testCases := []*types.Value{
		{Val: &types.Value_StringVal{StringVal: "test"}},
		{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}},
		{Val: &types.Value_Int32Val{Int32Val: 10}},
		{Val: &types.Value_Int64Val{Int64Val: 10}},
		{Val: &types.Value_FloatVal{FloatVal: 10.0}},
		{Val: &types.Value_DoubleVal{DoubleVal: 10.0}},
		{Val: &types.Value_BoolVal{BoolVal: true}},
		{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: timestamp.Unix()}},
		{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"a", "b", "c"}}}},
		{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: [][]byte{{1, 2}, {3, 4}}}}},
		{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{1, 2, 3}}}},
		{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{4, 5, 6}}}},
		{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: []float32{7.1, 8.2}}}},
		{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{9.3, 10.4}}}},
		{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: []bool{true, false}}}},
		{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{timestamp.Unix(), timestamp.Unix() + 3600}}}},
		{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
		nil,
		{},
		{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{}}}},
	}

	expectedTypes := []interface{}{
		"test",
		[]byte{1, 2, 3},
		int32(10),
		int64(10),
		float32(10.0),
		float64(10.0),
		true,
		timestamp,
		[]string{"a", "b", "c"},
		[][]byte{{1, 2}, {3, 4}},
		[]int32{1, 2, 3},
		[]int64{4, 5, 6},
		[]float32{7.1, 8.2},
		[]float64{9.3, 10.4},
		[]bool{true, false},
		[]time.Time{timestamp, timestamp.Add(3600 * time.Second)},
		nil,
		nil,
		nil,
		[]int32{},
	}

	for i, testCase := range testCases {
		actual := ValueTypeToGoType(testCase)
		assert.Equal(t, expectedTypes[i], actual, "Expected type mismatch for test case %d", i)
	}
}

func TestValueTypeToGoTypeTimestampAsString(t *testing.T) {
	timestamp := int64(1744769099)
	testCases := []*types.Value{
		{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: timestamp}},
		{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{timestamp, timestamp + 3600}}}},
		{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: math.MinInt64}},
		{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{timestamp, timestamp + 3600, math.MinInt64}}}},
	}

	expectedTypes := []interface{}{
		time.Unix(timestamp, 0).UTC().Format(TimestampFormat),
		[]string{
			time.Unix(timestamp, 0).UTC().Format(TimestampFormat),
			time.Unix(timestamp+3600, 0).UTC().Format(TimestampFormat),
		},
		"292277026596-12-04 15:30:08Z",
		[]string{
			time.Unix(timestamp, 0).UTC().Format(TimestampFormat),
			time.Unix(timestamp+3600, 0).UTC().Format(TimestampFormat),
			"292277026596-12-04 15:30:08Z",
		},
	}

	for i, testCase := range testCases {
		actual := ValueTypeToGoTypeTimestampAsString(testCase)
		assert.Equal(t, expectedTypes[i], actual)
	}
}

func TestConvertToValueType_String(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}, expected: "test"},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: ""}}, expected: ""},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_STRING)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetStringVal())
	}
}

func TestConvertToValueType_Bytes(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}}, expected: []byte{1, 2, 3}},
		{input: &types.Value{Val: &types.Value_BytesVal{BytesVal: nil}}, expected: []byte(nil)},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "\u0001\u0002\u0003"}}, expected: []byte{1, 2, 3}},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "dGVzdA=="}}, expected: []byte("test")},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_BYTES)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetBytesVal())
	}
}

func TestConvertToValueType_Int32(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_Int32Val{Int32Val: 10}}, expected: int32(10)},
		{input: &types.Value{Val: &types.Value_Int32Val{Int32Val: 0}}, expected: int32(0)},
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: 20}}, expected: int32(20)},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_INT32)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetInt32Val())
	}
}

func TestConvertToValueType_Int64(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: 10}}, expected: int64(10)},
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: 0}}, expected: int64(0)},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_INT64)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetInt64Val())
	}
}

func TestConvertToValueType_Float(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_FloatVal{FloatVal: 10.0}}, expected: float32(10.0)},
		{input: &types.Value{Val: &types.Value_FloatVal{FloatVal: 0.0}}, expected: float32(0.0)},
		{input: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 20.0}}, expected: float32(20.0)},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_FLOAT)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetFloatVal())
	}
}

func TestConvertToValueType_Double(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.0}}, expected: float64(10.0)},
		{input: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 0.0}}, expected: float64(0.0)},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_DOUBLE)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetDoubleVal())
	}
}

func TestConvertToValueType_Bool(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_BoolVal{BoolVal: true}}, expected: true},
		{input: &types.Value{Val: &types.Value_BoolVal{BoolVal: false}}, expected: false},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_BOOL)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetBoolVal())
	}
}

func TestConvertToValueType_Timestamp(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected interface{}
	}{
		{input: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: time.Now().Unix()}}, expected: time.Now().Unix()},
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: time.Now().Unix()}}, expected: time.Now().Unix()},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_UNIX_TIMESTAMP)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, result.GetUnixTimestampVal())
	}
}

func TestConvertToValueType_StringList(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected []string
	}{
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"a", "b", "c"}}}}, expected: []string{"a", "b", "c"}},
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{}}}}, expected: []string{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_STRING_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_StringListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetStringListVal().GetVal())
	}
}

func TestConvertToValueType_BytesList(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected [][]byte
	}{
		{input: &types.Value{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: [][]byte{{1, 2}, {3, 4}}}}}, expected: [][]byte{{1, 2}, {3, 4}}},
		{input: &types.Value{Val: &types.Value_BytesListVal{BytesListVal: &types.BytesList{Val: [][]byte{}}}}, expected: [][]byte{}},
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"\u0001\u0002", "\u0003\u0004"}}}}, expected: [][]byte{{1, 2}, {3, 4}}},
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"YQ==", "Yg==", "Yw=="}}}}, expected: [][]byte{[]byte("a"), []byte("b"), []byte("c")}},
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{}}}}, expected: [][]byte{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_BYTES_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_BytesListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetBytesListVal().GetVal())
	}
}

func TestConvertToValueType_Int32List(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected []int32
	}{
		{input: &types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{1, 2, 3}}}}, expected: []int32{1, 2, 3}},
		{input: &types.Value{Val: &types.Value_Int32ListVal{Int32ListVal: &types.Int32List{Val: []int32{}}}}, expected: []int32{}},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{1, 2, 3}}}}, expected: []int32{1, 2, 3}},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{}}}}, expected: []int32{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_INT32_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_Int32ListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetInt32ListVal().GetVal())
	}
}

func TestConvertToValueType_Int64List(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected []int64
	}{
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{1, 2, 3}}}}, expected: []int64{1, 2, 3}},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{}}}}, expected: []int64{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_INT64_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_Int64ListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetInt64ListVal().GetVal())
	}
}

func TestConvertToValueType_FloatList(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected []float32
	}{
		{input: &types.Value{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: []float32{1.1, 2.2, 3.3}}}}, expected: []float32{1.1, 2.2, 3.3}},
		{input: &types.Value{Val: &types.Value_FloatListVal{FloatListVal: &types.FloatList{Val: []float32{}}}}, expected: []float32{}},
		{input: &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{1.1, 2.2, 3.3}}}}, expected: []float32{1.1, 2.2, 3.3}},
		{input: &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{}}}}, expected: []float32{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_FLOAT_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_FloatListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetFloatListVal().GetVal())
	}
}

func TestConvertToValueType_DoubleList(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected []float64
	}{
		{input: &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{1.1, 2.2, 3.3}}}}, expected: []float64{1.1, 2.2, 3.3}},
		{input: &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{}}}}, expected: []float64{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_DOUBLE_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_DoubleListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetDoubleListVal().GetVal())
	}
}

func TestConvertToValueType_UnixTimestampList(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected []int64
	}{
		{input: &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{1622547800, 1622547900}}}}, expected: []int64{1622547800, 1622547900}},
		{input: &types.Value{Val: &types.Value_UnixTimestampListVal{UnixTimestampListVal: &types.Int64List{Val: []int64{}}}}, expected: []int64{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_UNIX_TIMESTAMP_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_UnixTimestampListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetUnixTimestampListVal().GetVal())
	}
}

func TestConvertToValueType_BoolList(t *testing.T) {
	testCases := []struct {
		input    *types.Value
		expected []bool
	}{
		{input: &types.Value{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: []bool{true, false}}}}, expected: []bool{true, false}},
		{input: &types.Value{Val: &types.Value_BoolListVal{BoolListVal: &types.BoolList{Val: []bool{}}}}, expected: []bool{}},
	}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc.input, types.ValueType_BOOL_LIST)
		assert.NoErrorf(t, err, "Error converting value: %v", tc.input)
		assert.IsType(t, &types.Value_BoolListVal{}, result.GetVal())
		assert.ElementsMatch(t, tc.expected, result.GetBoolListVal().GetVal())
	}
}

func TestConvertToValueType_ValidNulls(t *testing.T) {
	testCases := []*types.Value{{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}, {}, nil}

	for _, tc := range testCases {
		result, err := ConvertToValueType(tc, types.ValueType_NULL)
		assert.NoErrorf(t, err, "Expected no error converting value: %v", tc)
		assert.Nil(t, result, "Expected nil result for value: %v", tc)
	}
}

func TestConvertToValueType_InvalidNulls(t *testing.T) {
	testCases := []*types.Value{{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}, {}, nil}

	for _, tc := range testCases {
		_, err := ConvertToValueType(tc, types.ValueType_STRING)
		assert.Error(t, err, "Expected error converting value: %v", tc)
		assert.Equal(t, "value is nil, cannot convert to type STRING", err.Error())
	}
}

func TestConvertToValueType_InvalidType(t *testing.T) {
	errorStr := "unsupported value type for conversion: %s for actual value type: %s"
	testCases := []struct {
		input     *types.Value
		valueType types.ValueType_Enum
		expected  string
	}{
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: 10}}, valueType: types.ValueType_STRING, expected: fmt.Sprintf(errorStr, "STRING", "*types.Value_Int64Val")},
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: 10}}, valueType: types.ValueType_BYTES, expected: fmt.Sprintf(errorStr, "BYTES", "*types.Value_Int64Val")},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}, valueType: types.ValueType_INT32, expected: fmt.Sprintf(errorStr, "INT32", "*types.Value_StringVal")},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}, valueType: types.ValueType_INT64, expected: fmt.Sprintf(errorStr, "INT64", "*types.Value_StringVal")},
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: 10}}, valueType: types.ValueType_FLOAT, expected: fmt.Sprintf(errorStr, "FLOAT", "*types.Value_Int64Val")},
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: 10}}, valueType: types.ValueType_DOUBLE, expected: fmt.Sprintf(errorStr, "DOUBLE", "*types.Value_Int64Val")},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}, valueType: types.ValueType_UNIX_TIMESTAMP, expected: fmt.Sprintf(errorStr, "UNIX_TIMESTAMP", "*types.Value_StringVal")},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}, valueType: types.ValueType_BOOL, expected: fmt.Sprintf(errorStr, "BOOL", "*types.Value_StringVal")},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{10}}}}, valueType: types.ValueType_STRING_LIST, expected: fmt.Sprintf(errorStr, "STRING_LIST", "*types.Value_Int64ListVal")},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{10}}}}, valueType: types.ValueType_BYTES_LIST, expected: fmt.Sprintf(errorStr, "BYTES_LIST", "*types.Value_Int64ListVal")},
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"test"}}}}, valueType: types.ValueType_INT32_LIST, expected: fmt.Sprintf(errorStr, "INT32_LIST", "*types.Value_StringListVal")},
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"test"}}}}, valueType: types.ValueType_INT64_LIST, expected: fmt.Sprintf(errorStr, "INT64_LIST", "*types.Value_StringListVal")},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{10}}}}, valueType: types.ValueType_FLOAT_LIST, expected: fmt.Sprintf(errorStr, "FLOAT_LIST", "*types.Value_Int64ListVal")},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{10}}}}, valueType: types.ValueType_DOUBLE_LIST, expected: fmt.Sprintf(errorStr, "DOUBLE_LIST", "*types.Value_Int64ListVal")},
		{input: &types.Value{Val: &types.Value_StringListVal{StringListVal: &types.StringList{Val: []string{"test"}}}}, valueType: types.ValueType_UNIX_TIMESTAMP_LIST, expected: fmt.Sprintf(errorStr, "UNIX_TIMESTAMP_LIST", "*types.Value_StringListVal")},
		{input: &types.Value{Val: &types.Value_StringVal{StringVal: "test"}}, valueType: types.ValueType_BOOL_LIST, expected: fmt.Sprintf(errorStr, "BOOL_LIST", "*types.Value_StringVal")},
	}

	for _, tc := range testCases {
		_, err := ConvertToValueType(tc.input, tc.valueType)
		assert.Error(t, err, "Expected error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, err.Error(), "Error message mismatch for input: %v", tc.input)
	}
}

func TestConvertToValueType_OutOfBoundValues(t *testing.T) {
	errorStrI := "value %d is out of range for %s"
	errorStrE := "value %e is out of range for %s"
	testCases := []struct {
		input     *types.Value
		valueType types.ValueType_Enum
		expected  string
	}{
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: math.MaxInt64}}, valueType: types.ValueType_INT32, expected: fmt.Sprintf(errorStrI, math.MaxInt64, "INT32")},
		{input: &types.Value{Val: &types.Value_Int64Val{Int64Val: math.MinInt64}}, valueType: types.ValueType_INT32, expected: fmt.Sprintf(errorStrI, math.MinInt64, "INT32")},
		{input: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: math.MaxFloat64}}, valueType: types.ValueType_FLOAT, expected: fmt.Sprintf(errorStrE, math.MaxFloat64, "FLOAT")},
		{input: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: math.SmallestNonzeroFloat64}}, valueType: types.ValueType_FLOAT, expected: fmt.Sprintf(errorStrE, math.SmallestNonzeroFloat64, "FLOAT")},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{math.MaxInt64}}}}, valueType: types.ValueType_INT32_LIST, expected: fmt.Sprintf(errorStrI, math.MaxInt64, "INT32_LIST")},
		{input: &types.Value{Val: &types.Value_Int64ListVal{Int64ListVal: &types.Int64List{Val: []int64{math.MinInt64}}}}, valueType: types.ValueType_INT32_LIST, expected: fmt.Sprintf(errorStrI, math.MinInt64, "INT32_LIST")},
		{input: &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{math.MaxFloat64}}}}, valueType: types.ValueType_FLOAT_LIST, expected: fmt.Sprintf(errorStrE, math.MaxFloat64, "FLOAT_LIST")},
		{input: &types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{math.SmallestNonzeroFloat64}}}}, valueType: types.ValueType_FLOAT_LIST, expected: fmt.Sprintf(errorStrE, math.SmallestNonzeroFloat64, "FLOAT_LIST")},
	}

	for _, tc := range testCases {
		_, err := ConvertToValueType(tc.input, tc.valueType)
		assert.Error(t, err, "Expected error converting value: %v", tc.input)
		assert.Equal(t, tc.expected, err.Error(), "Error message mismatch for input: %v", tc.input)
	}
}
