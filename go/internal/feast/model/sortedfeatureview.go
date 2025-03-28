package model

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	types2 "github.com/feast-dev/feast/go/types"
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
	if len(proto.Spec.Entities) == 0 {
		baseFV.EntityNames = []string{DUMMY_ENTITY_NAME}
	} else {
		baseFV.EntityNames = proto.Spec.Entities
	}
	entityColumns := make([]*Field, len(proto.Spec.EntityColumns))
	for i, entityColumn := range proto.Spec.EntityColumns {
		entityColumns[i] = NewFieldFromProto(entityColumn)
	}
	baseFV.EntityColumns = entityColumns

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

type SortKeyFilter struct {
	SortKeyName    string
	RangeStart     interface{}
	RangeEnd       interface{}
	StartInclusive bool
	EndInclusive   bool
	Order          *SortOrder
}

func NewSortKeyFilterFromProto(proto *serving.SortKeyFilter, sortOrder core.SortOrder_Enum) *SortKeyFilter {
	return &SortKeyFilter{
		SortKeyName:    proto.GetSortKeyName(),
		RangeStart:     types2.ValueTypeToGoType(proto.GetRangeStart()),
		RangeEnd:       types2.ValueTypeToGoType(proto.GetRangeEnd()),
		StartInclusive: proto.GetStartInclusive(),
		EndInclusive:   proto.GetEndInclusive(),
		Order:          NewSortOrderFromProto(sortOrder),
	}
}
