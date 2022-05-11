package types

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/feast-dev/feast/go/protos/feast/types"
)

var (
	PROTO_VALUES = [][]*types.Value{
		{{Val: &types.Value_Int32Val{10}}, {Val: &types.Value_Int32Val{20}}},
		{{Val: &types.Value_Int64Val{10}}, {Val: &types.Value_Int64Val{20}}},
		{{Val: &types.Value_FloatVal{1.0}}, {Val: &types.Value_FloatVal{2.0}}},
		{{Val: &types.Value_DoubleVal{1.0}}, {Val: &types.Value_DoubleVal{2.0}}},
		{{Val: &types.Value_StringVal{"aaa"}}, {Val: &types.Value_StringVal{"bbb"}}},
		{{Val: &types.Value_BytesVal{[]byte{1, 2, 3}}}, {Val: &types.Value_BytesVal{[]byte{4, 5, 6}}}},
		{{Val: &types.Value_BoolVal{true}}, {Val: &types.Value_BoolVal{false}}},
		{{Val: &types.Value_UnixTimestampVal{time.Now().Unix()}},
			{Val: &types.Value_UnixTimestampVal{time.Now().Unix()}}},

		{
			{Val: &types.Value_Int32ListVal{&types.Int32List{Val: []int32{0, 1, 2}}}},
			{Val: &types.Value_Int32ListVal{&types.Int32List{Val: []int32{3, 4, 5}}}},
		},
		{
			{Val: &types.Value_Int64ListVal{&types.Int64List{Val: []int64{0, 1, 2}}}},
			{Val: &types.Value_Int64ListVal{&types.Int64List{Val: []int64{3, 4, 5}}}},
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
