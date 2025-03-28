package types

import (
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
		{Val: []*types.Value{}},
		{Val: []*types.Value{nil_or_null_val}},
		{Val: []*types.Value{nil_or_null_val, nil_or_null_val}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, {Val: &types.Value_Int32Val{Int32Val: 20}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, nil_or_null_val}},
		{Val: []*types.Value{nil_or_null_val, {Val: &types.Value_Int32Val{Int32Val: 20}}}},
		{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}, nil_or_null_val, {Val: &types.Value_Int32Val{Int32Val: 30}}}},
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
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}}},
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 20}}}},
		},
		{
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 10}}}},
			{Val: []*types.Value{nil_or_null_val}},
			{Val: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 30}}}},
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
			{Val: []*types.Value{nil_or_null_val}},
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
				nil_or_null_val,
				{Val: &types.Value_Int32Val{Int32Val: 30}},
			}},
			{Val: []*types.Value{nil_or_null_val}},
			{Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 40}},
				{Val: &types.Value_Int32Val{Int32Val: 50}},
			}},
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

func TestRepeatedValueRoundTrip(t *testing.T) {
	pool := memory.NewGoAllocator()

	for i, repeatedValue := range REPEATED_PROTO_VALUES {
		arrowArray, err := RepeatedProtoValuesToArrowArray([]*types.RepeatedValue{repeatedValue}, pool, 1)
		assert.Nil(t, err, "Error creating Arrow array for case %d", i)

		result, err := ArrowValuesToRepeatedProtoValues(arrowArray)
		assert.Nil(t, err, "Error converting back for case %d", i)

		assert.Equal(t, 1, len(result), "Should have 1 result for case %d", i)

		if len(repeatedValue.Val) == 0 {
			if len(result[0].Val) > 1 {
				t.Errorf("Case %d: Expected empty value or single null, got %d values",
					i, len(result[0].Val))
			} else if len(result[0].Val) == 1 && result[0].Val[0] != nil && result[0].Val[0].Val != nil {
				t.Errorf("Case %d: Expected null value, got a non-null value", i)
			}
			continue
		}

		for j := 0; j < len(repeatedValue.Val); j++ {
			if repeatedValue.Val[j] != nil && repeatedValue.Val[j].Val != nil {
				if j >= len(result[0].Val) {
					continue
				}

				switch v := repeatedValue.Val[j].Val.(type) {
				case *types.Value_FloatVal:
					if math.IsNaN(float64(v.FloatVal)) {
						assert.True(t, math.IsNaN(float64(result[0].Val[j].GetFloatVal())),
							"Float NaN not preserved at index %d in case %d", j, i)
					} else {
						assert.Equal(t, v.FloatVal, result[0].Val[j].GetFloatVal(),
							"Float value mismatch at index %d in case %d", j, i)
					}
				case *types.Value_DoubleVal:
					if math.IsNaN(v.DoubleVal) {
						assert.True(t, math.IsNaN(result[0].Val[j].GetDoubleVal()),
							"Double NaN not preserved at index %d in case %d", j, i)
					} else {
						assert.Equal(t, v.DoubleVal, result[0].Val[j].GetDoubleVal(),
							"Double value mismatch at index %d in case %d", j, i)
					}
				default:
					assert.True(t, proto.Equal(repeatedValue.Val[j], result[0].Val[j]),
						"Value mismatch at index %d in case %d", j, i)
				}
			}
		}
	}
}

func TestMultipleRepeatedValueRoundTrip(t *testing.T) {
	pool := memory.NewGoAllocator()

	for i, batch := range MULTIPLE_REPEATED_PROTO_VALUES {
		arrowArray, err := RepeatedProtoValuesToArrowArray(batch, pool, len(batch))
		assert.Nil(t, err, "Error creating Arrow array for batch %d", i)

		results, err := ArrowValuesToRepeatedProtoValues(arrowArray)
		assert.Nil(t, err, "Error converting back for batch %d", i)

		assert.Equal(t, len(batch), len(results),
			"Row count mismatch for batch %d", i)

		for j := 0; j < len(batch); j++ {
			original := batch[j]
			result := results[j]

			if len(original.Val) == 0 {
				if len(result.Val) > 1 {
					t.Errorf("Batch %d, row %d: Expected empty value or single null, got %d values",
						i, j, len(result.Val))
				} else if len(result.Val) == 1 && result.Val[0] != nil && result.Val[0].Val != nil {
					t.Errorf("Batch %d, row %d: Expected null value, got a non-null value", i, j)
				}
				continue
			}

			for k := 0; k < len(original.Val); k++ {
				if original.Val[k] != nil && original.Val[k].Val != nil {
					if k >= len(result.Val) {
						continue
					}

					switch v := original.Val[k].Val.(type) {
					case *types.Value_FloatVal:
						if math.IsNaN(float64(v.FloatVal)) {
							assert.True(t, math.IsNaN(float64(result.Val[k].GetFloatVal())),
								"Float NaN not preserved in batch %d, row %d, index %d", i, j, k)
						} else {
							assert.Equal(t, v.FloatVal, result.Val[k].GetFloatVal(),
								"Float value mismatch in batch %d, row %d, index %d", i, j, k)
						}
					case *types.Value_DoubleVal:
						if math.IsNaN(v.DoubleVal) {
							assert.True(t, math.IsNaN(result.Val[k].GetDoubleVal()),
								"Double NaN not preserved in batch %d, row %d, index %d", i, j, k)
						} else {
							assert.Equal(t, v.DoubleVal, result.Val[k].GetDoubleVal(),
								"Double value mismatch in batch %d, row %d, index %d", i, j, k)
						}
					default:
						assert.True(t, proto.Equal(original.Val[k], result.Val[k]),
							"Value mismatch in batch %d, row %d, index %d", i, j, k)
					}
				}
			}
		}
	}
}

func TestEmptyAndNullRepeatedValues(t *testing.T) {
	pool := memory.NewGoAllocator()

	testCases := [][]*types.RepeatedValue{
		{},
		{{Val: []*types.Value{}}},
		{{Val: []*types.Value{}}, {Val: []*types.Value{}}},
		{{Val: []*types.Value{nil_or_null_val}}},
		{{Val: []*types.Value{nil_or_null_val, nil_or_null_val}}},
		{{Val: []*types.Value{}}, {Val: []*types.Value{nil_or_null_val}}},
	}

	for i, testCase := range testCases {
		arrowArray, err := RepeatedProtoValuesToArrowArray(testCase, pool, len(testCase))
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

func TestProtoValuesToRepeatedConversion(t *testing.T) {
	pool := memory.NewGoAllocator()

	testCases := [][]*types.Value{
		{{Val: &types.Value_Int32Val{Int32Val: 10}}, {Val: &types.Value_Int32Val{Int32Val: 20}}},
		{{Val: &types.Value_StringVal{StringVal: "test"}}},
		{nil_or_null_val, {Val: &types.Value_BoolVal{BoolVal: true}}},
	}

	for i, protoValues := range testCases {
		arrowArray, err := ProtoValuesToArrowArray(protoValues, pool, len(protoValues))
		assert.Nil(t, err, "Error creating Arrow array for case %d", i)

		result, err := ArrowValuesToRepeatedProtoValues(arrowArray)
		assert.Nil(t, err, "Error converting to RepeatedProtoValues for case %d", i)
		assert.Equal(t, len(protoValues), len(result),
			"Result count mismatch for case %d", i)

		for j := 0; j < len(protoValues); j++ {
			if protoValues[j] != nil && protoValues[j].Val != nil {
				assert.Equal(t, 1, len(result[j].Val),
					"Expected single value in RepeatedValue for case %d, row %d", i, j)

				if len(result[j].Val) > 0 {
					assert.True(t, proto.Equal(protoValues[j], result[j].Val[0]),
						"Value mismatch in case %d, row %d", i, j)
				}
			}
		}
	}
}

func TestValueTypeToGoType(t *testing.T) {
	timestamp := time.Now().Unix()
	testCases := []*types.Value{
		{Val: &types.Value_StringVal{StringVal: "test"}},
		{Val: &types.Value_BytesVal{BytesVal: []byte{1, 2, 3}}},
		{Val: &types.Value_Int32Val{Int32Val: 10}},
		{Val: &types.Value_Int64Val{Int64Val: 10}},
		{Val: &types.Value_FloatVal{FloatVal: 10.0}},
		{Val: &types.Value_DoubleVal{DoubleVal: 10.0}},
		{Val: &types.Value_BoolVal{BoolVal: true}},
		{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: timestamp}},
		{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
		nil,
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
		nil,
		nil,
	}

	for i, testCase := range testCases {
		actual := ValueTypeToGoType(testCase)
		assert.Equal(t, expectedTypes[i], actual)
	}
}
