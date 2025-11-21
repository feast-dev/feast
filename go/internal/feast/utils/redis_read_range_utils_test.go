package utils

import (
	"testing"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBuildZsetKey(t *testing.T) {
	key := BuildZsetKey("fv", []byte{0x01, 0x02})
	assert.Equal(t, "fv\x01\x02", key)
}

func TestBuildHashKey(t *testing.T) {
	k := BuildHashKey([]byte{1, 2}, []byte{3, 4})
	assert.Equal(t, string([]byte{1, 2, 3, 4}), k)
}

func TestDecodeTimestamp(t *testing.T) {
	ts := timestamppb.Now()
	b, _ := proto.Marshal(ts)

	out := DecodeTimestamp(b)
	assert.Equal(t, ts.AsTime().Unix(), out.AsTime().Unix())

	out2 := DecodeTimestamp(nil)
	assert.Equal(t, int64(0), out2.AsTime().Unix())
}

func TestMmh3FieldHash(t *testing.T) {
	h1 := Mmh3FieldHash("driver_fv", "trip_count")
	h2 := Mmh3FieldHash("driver_fv", "trip_count")
	h3 := Mmh3FieldHash("other_fv", "trip_count")

	require.Equal(t, 4, len(h1))
	assert.Equal(t, h1, h2)
	assert.NotEqual(t, h1, h3)
	assert.NotEqual(t, "\x00\x00\x00\x00", h1)
}

func TestGetScoreRange(t *testing.T) {
	ptrInt := func(v int64) *int64 { return &v }

	cases := []struct {
		name   string
		filter []*model.SortKeyFilter
		min    string
		max    string
	}{
		{
			"no filters",
			[]*model.SortKeyFilter{},
			"-inf", "+inf",
		},
		{
			"equals native",
			[]*model.SortKeyFilter{{Equals: int64(100)}},
			"100", "100",
		},
		{
			"equals pointer",
			[]*model.SortKeyFilter{{Equals: ptrInt(200)}},
			"200", "200",
		},
		{
			"range start inclusive",
			[]*model.SortKeyFilter{{RangeStart: int64(10), StartInclusive: true}},
			"10", "+inf",
		},
		{
			"range end exclusive",
			[]*model.SortKeyFilter{{RangeEnd: int64(50), EndInclusive: false}},
			"-inf", "(50",
		},
		{
			"time.Time",
			[]*model.SortKeyFilter{
				{
					RangeStart:     time.Unix(1000, 0),
					RangeEnd:       time.Unix(2000, 0),
					StartInclusive: true,
					EndInclusive:   true,
				}},
			"1000", "2000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			min, max := GetScoreRange(tc.filter)
			assert.Equal(t, tc.min, min)
			assert.Equal(t, tc.max, max)
		})
	}
}

func TestEffectiveReverse_ASC_Default_NoUserReverse(t *testing.T) {
	filter := &model.SortKeyFilter{
		SortKeyName:  "sort1",
		RangeEnd:     12,
		EndInclusive: true,
		Order:        &model.SortOrder{Order: core.SortOrder_ASC},
	}

	got := ComputeEffectiveReverse([]*model.SortKeyFilter{filter}, false)

	if got {
		t.Fatalf("ASC + userReverse=false: expected Rev=false, got true")
	}
}

func TestEffectiveReverse_ASC_WithUserReverse(t *testing.T) {
	filter := &model.SortKeyFilter{
		SortKeyName:  "sort1",
		RangeEnd:     12,
		EndInclusive: true,
		Order:        &model.SortOrder{Order: core.SortOrder_ASC},
	}

	got := ComputeEffectiveReverse([]*model.SortKeyFilter{filter}, true)

	if !got {
		t.Fatalf("ASC + userReverse=true: expected Rev=true, got false")
	}
}

func TestEffectiveReverse_DESC_Default_NoUserReverse(t *testing.T) {
	filter := &model.SortKeyFilter{
		SortKeyName:  "sort1",
		RangeEnd:     12,
		EndInclusive: true,
		Order:        &model.SortOrder{Order: core.SortOrder_DESC},
	}

	got := ComputeEffectiveReverse([]*model.SortKeyFilter{filter}, false)

	// Expect true: DESC requires flipping once
	if !got {
		t.Fatalf("DESC + userReverse=false: expected Rev=true, got false")
	}
}

func TestEffectiveReverse_DESC_WithUserReverse(t *testing.T) {
	filter := &model.SortKeyFilter{
		SortKeyName:  "sort1",
		RangeEnd:     12,
		EndInclusive: true,
		Order:        &model.SortOrder{Order: core.SortOrder_DESC},
	}

	got := ComputeEffectiveReverse([]*model.SortKeyFilter{filter}, true)

	if got {
		t.Fatalf("DESC + userReverse=true: expected Rev=false, got true")
	}
}

func TestEffectiveReverse_NoFilters(t *testing.T) {
	got := ComputeEffectiveReverse(nil, false)
	if got {
		t.Fatalf("no filters + userReverse=false: expected Rev=false, got true")
	}

	got = ComputeEffectiveReverse(nil, true)
	if !got {
		t.Fatalf("no filters + userReverse=true: expected Rev=true, got false")
	}
}

func TestEffectiveReverse_FilterWithNilOrder(t *testing.T) {
	filter := &model.SortKeyFilter{
		SortKeyName: "sort1",
		RangeEnd:    12,
	}

	got := ComputeEffectiveReverse([]*model.SortKeyFilter{filter}, false)
	if got {
		t.Fatalf("nil order + userReverse=false: expected Rev=false, got true")
	}

	got = ComputeEffectiveReverse([]*model.SortKeyFilter{filter}, true)
	if !got {
		t.Fatalf("nil order + userReverse=true: expected Rev=true, got false")
	}
}

func TestDecodeFeatureValue(t *testing.T) {
	valPb := &types.Value{Val: &types.Value_StringVal{StringVal: "hello"}}
	b, _ := proto.Marshal(valPb)

	val, status := DecodeFeatureValue(b, "fv", "feat", "member")
	assert.Equal(t, serving.FieldStatus_PRESENT, status)
	assert.Equal(t, "hello", val.(*types.Value).GetStringVal())

	_, st2 := DecodeFeatureValue(nil, "fv", "feat", "m")
	assert.Equal(t, serving.FieldStatus_NULL_VALUE, st2)
}
