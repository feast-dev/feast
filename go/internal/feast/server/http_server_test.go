package server

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshalJSON(t *testing.T) {
	u := repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1, 2, 3]")))
	assert.Equal(t, []int64{1, 2, 3}, u.int64Val)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1.2, 2.3, 3.4]")))
	assert.Equal(t, []float64{1.2, 2.3, 3.4}, u.doubleVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[\"foo\", \"bar\"]")))
	assert.Equal(t, []string{"foo", "bar"}, u.stringVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[true, false, true]")))
	assert.Equal(t, []bool{true, false, true}, u.boolVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1, 2, 3], [4, 5, 6]]")))
	assert.Equal(t, [][]int64{{1, 2, 3}, {4, 5, 6}}, u.int64ListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1.2, 2.3, 3.4], [10.2, 20.3, 30.4]]")))
	assert.Equal(t, [][]float64{{1.2, 2.3, 3.4}, {10.2, 20.3, 30.4}}, u.doubleListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[\"foo\", \"bar\"], [\"foo2\", \"bar2\"]]")))
	assert.Equal(t, [][]string{{"foo", "bar"}, {"foo2", "bar2"}}, u.stringListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[true, false, true], [false, true, false]]")))
	assert.Equal(t, [][]bool{{true, false, true}, {false, true, false}}, u.boolListVal)
}
func TestMarshalInt32JSON(t *testing.T) {
	var arrowArray arrow.Array
	memoryPool := memory.NewGoAllocator()
	builder := array.NewInt32Builder(memoryPool)
	defer builder.Release()
	builder.AppendValues([]int32{1, 2, 3, 4}, nil)
	arrowArray = builder.NewArray()
	defer arrowArray.Release()
	expectedJSON := `[1,2,3,4]`

	jsonData, err := json.Marshal(arrowArray)
	assert.NoError(t, err, "Error marshaling Arrow array")

	assert.Equal(t, expectedJSON, string(jsonData), "JSON output does not match expected")
	assert.IsType(t, &array.Int32{}, arrowArray, "arrowArray is not of type *array.Int32")
}

func TestMarshalInt64JSON(t *testing.T) {
	var arrowArray arrow.Array
	memoryPool := memory.NewGoAllocator()
	builder := array.NewInt64Builder(memoryPool)
	defer builder.Release()
	builder.AppendValues([]int64{-9223372036854775808, 9223372036854775807}, nil)
	arrowArray = builder.NewArray()
	defer arrowArray.Release()
	expectedJSON := `[-9223372036854775808,9223372036854775807]`

	jsonData, err := json.Marshal(arrowArray)
	assert.NoError(t, err, "Error marshaling Arrow array")

	assert.Equal(t, expectedJSON, string(jsonData), "JSON output does not match expected")
	assert.IsType(t, &array.Int64{}, arrowArray, "arrowArray is not of type *array.Int64")
}
