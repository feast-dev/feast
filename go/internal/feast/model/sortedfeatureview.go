package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type SortOrder struct {
	Order core.SortOrder_Enum
}

func NewSortOrderFromProto(order core.SortOrder_Enum) *SortOrder {
	return &SortOrder{
		Order: order,
	}
}

func (so *SortOrder) String() string {
	switch so.Order {
	case core.SortOrder_ASC:
		return "ASC"
	case core.SortOrder_DESC:
		return "DESC"
	default:
		return "INVALID"
	}
}

func (so *SortOrder) ToProto() core.SortOrder_Enum {
	return so.Order
}

type SortKey struct {
	FieldName string
	Order     *SortOrder
	ValueType types.ValueType_Enum
}

func NewSortKeyFromProto(proto *core.SortKey) *SortKey {
	return &SortKey{
		FieldName: proto.GetName(),
		Order:     NewSortOrderFromProto(proto.GetDefaultSortOrder()),
		ValueType: proto.GetValueType(),
	}
}

type SortedFeatureView struct {
	*FeatureView
	SortKeys []*SortKey
}

func NewSortedFeatureViewFromProto(proto *core.SortedFeatureView) *SortedFeatureView {
	baseFV := &FeatureView{
		Base: NewBaseFeatureView(proto.GetSpec().GetName(), proto.GetSpec().GetFeatures()),
		Ttl:  proto.GetSpec().GetTtl(),
	}

	// Convert each sort key from the proto.
	sortKeys := make([]*SortKey, len(proto.GetSpec().GetSortKeys()))
	for i, skProto := range proto.GetSpec().GetSortKeys() {
		sortKeys[i] = NewSortKeyFromProto(skProto)
	}

	return &SortedFeatureView{
		FeatureView: baseFV,
		SortKeys:    sortKeys,
	}
}

func (sfv *SortedFeatureView) NewSortedFeatureViewFromBase(base *BaseFeatureView) *SortedFeatureView {
	newFV := sfv.FeatureView.NewFeatureViewFromBase(base)
	return &SortedFeatureView{
		FeatureView: newFV,
		SortKeys:    sfv.SortKeys,
	}
}
