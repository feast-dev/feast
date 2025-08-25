package test

import (
	"github.com/feast-dev/feast/go/internal/feast/model"
	"testing"

	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
)

func TestNewSortOrderFromProtoPtr(t *testing.T) {

	assert.Nil(t, model.NewSortOrderFromProtoPtr(nil))

	asc := core.SortOrder_ASC
	so := model.NewSortOrderFromProtoPtr(&asc)
	if assert.NotNil(t, so) {
		assert.Equal(t, asc, so.Order)
		assert.Equal(t, "ASC", so.String())
		assert.Equal(t, asc, so.ToProto())
	}
}

func TestNewSortKeyFilterFromProto_Equals(t *testing.T) {
	eqVal := &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}}

	req := &serving.SortKeyFilter{
		SortKeyName: "price",
		Query:       &serving.SortKeyFilter_Equals{Equals: eqVal},
	}

	asc := core.SortOrder_ASC
	f := model.NewSortKeyFilterFromProto(req, &asc)

	assert.Equal(t, "price", f.SortKeyName)
	assert.EqualValues(t, 42, f.Equals)
	assert.Nil(t, f.RangeStart)
	assert.Equal(t, asc, f.Order.Order)
}

func TestNewSortKeyFilterFromProto_Range(t *testing.T) {
	rq := &serving.SortKeyFilter_RangeQuery{
		RangeStart:     &types.Value{Val: &types.Value_Int64Val{Int64Val: 100}},
		RangeEnd:       &types.Value{Val: &types.Value_Int64Val{Int64Val: 200}},
		StartInclusive: true,
		EndInclusive:   false,
	}
	req := &serving.SortKeyFilter{
		SortKeyName: "ts",
		Query:       &serving.SortKeyFilter_Range{Range: rq},
	}

	desc := core.SortOrder_DESC
	f := model.NewSortKeyFilterFromProto(req, &desc)

	assert.Equal(t, int64(100), f.RangeStart)
	assert.Equal(t, int64(200), f.RangeEnd)
	assert.True(t, f.StartInclusive)
	assert.False(t, f.EndInclusive)
	assert.Equal(t, desc, f.Order.Order)
}

func TestNewSortKeyFilterFromProto_NoOrderOverride(t *testing.T) {
	req := &serving.SortKeyFilter{
		SortKeyName: "id",
		Query: &serving.SortKeyFilter_Equals{
			Equals: &types.Value{Val: &types.Value_Int32Val{Int32Val: 7}},
		},
	}

	f := model.NewSortKeyFilterFromProto(req, nil)

	assert.Equal(t, "id", f.SortKeyName)
	assert.EqualValues(t, 7, f.Equals)
	assert.Nil(t, f.Order)
}
