package onlineserving

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func TestGroupingFeatureRefs(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				NameAlias: "aliasViewA",
			},
		},
		EntityNames: []string{"driver", "customer"},
	}
	viewB := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewB"},
		EntityNames: []string{"driver", "customer"},
	}
	viewC := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewC"},
		EntityNames: []string{"driver"},
	}
	viewD := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewD"},
		EntityNames: []string{"customer"},
	}
	refGroups, _ := GroupFeatureRefs(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
			{View: viewC, FeatureRefs: []string{"featureE"}},
			{View: viewD, FeatureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		true,
	)

	assert.Len(t, refGroups, 3)

	// Group 1
	assert.Equal(t, []string{"featureA", "featureB", "featureC", "featureD"},
		refGroups["customer_id,driver_id"].FeatureNames)
	assert.Equal(t, []string{"viewA", "viewA", "viewB", "viewB"},
		refGroups["customer_id,driver_id"].FeatureViewNames)
	assert.Equal(t, []string{
		"aliasViewA__featureA", "aliasViewA__featureB",
		"viewB__featureC", "viewB__featureD"},
		refGroups["customer_id,driver_id"].AliasedFeatureNames)
	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["customer_id,driver_id"].Indices, group)
	}

	// Group2
	assert.Equal(t, []string{"featureE"},
		refGroups["driver_id"].FeatureNames)
	for _, group := range [][]int{{0, 1}, {2, 3, 4}} {
		assert.Contains(t, refGroups["driver_id"].Indices, group)
	}

	// Group3
	assert.Equal(t, []string{"featureF"},
		refGroups["customer_id"].FeatureNames)

	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["customer_id"].Indices, group)
	}

}

func TestGroupingFeatureRefsWithJoinKeyAliases(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		EntityNames: []string{"location"},
	}
	viewB := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewB"},
		EntityNames: []string{"location"},
	}

	refGroups, _ := GroupFeatureRefs(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"destination_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
	)

	assert.Len(t, refGroups, 2)

	assert.Equal(t, []string{"featureA", "featureB"},
		refGroups["location_id[destination_id]"].FeatureNames)
	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["location_id[destination_id]"].Indices, group)
	}

	assert.Equal(t, []string{"featureC", "featureD"},
		refGroups["location_id"].FeatureNames)
	for _, group := range [][]int{{0, 1}, {2, 3, 4}} {
		assert.Contains(t, refGroups["location_id"].Indices, group)
	}

}

func TestGroupingFeatureRefsWithMissingKey(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		EntityNames: []string{"location"},
	}

	_, err := GroupFeatureRefs(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
	)
	assert.Errorf(t, err, "key destination_id is missing in provided entity rows for view viewA")
}

func createFeature(name string, valueType types.ValueType_Enum) *core.FeatureSpecV2 {
	return &core.FeatureSpecV2{
		Name:      name,
		ValueType: valueType,
	}
}

func createFeatureView(name string, entities []string, features ...*core.FeatureSpecV2) *model.FeatureView {
	viewProto := core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name:     name,
			Entities: entities,
			Features: features,
			Ttl:      &durationpb.Duration{},
		},
	}
	return model.NewFeatureViewFromProto(&viewProto)
}

func createSortKey(name string, order core.SortOrder_Enum, valueType types.ValueType_Enum) *core.SortKey {
	return &core.SortKey{
		Name:             name,
		DefaultSortOrder: order,
		ValueType:        valueType,
	}
}

func createSortedFeatureView(name string, entities []string, sortKeys []*core.SortKey, features ...*core.FeatureSpecV2) *model.SortedFeatureView {
	viewProto := core.SortedFeatureView{
		Spec: &core.SortedFeatureViewSpec{
			Name:     name,
			Entities: entities,
			Features: features,
			SortKeys: sortKeys,
			Ttl:      &durationpb.Duration{},
		},
	}
	return model.NewSortedFeatureViewFromProto(&viewProto)
}

func createFeatureService(viewProjections map[string][]*core.FeatureSpecV2) *model.FeatureService {
	projections := make([]*core.FeatureViewProjection, 0)
	for name, features := range viewProjections {
		projections = append(projections, &core.FeatureViewProjection{
			FeatureViewName: name,
			FeatureColumns:  features,
			JoinKeyMap:      map[string]string{},
		})
	}

	fsProto := core.FeatureService{
		Spec: &core.FeatureServiceSpec{
			Features: projections,
		},
		Meta: &core.FeatureServiceMeta{
			LastUpdatedTimestamp: timestamppb.Now(),
			CreatedTimestamp:     timestamppb.Now(),
		},
	}

	return model.NewFeatureServiceFromProto(&fsProto)
}

func createOnDemandFeatureView(name string, featureSources map[string][]*core.FeatureSpecV2, features ...*core.FeatureSpecV2) *model.OnDemandFeatureView {
	sources := make(map[string]*core.OnDemandSource)
	for viewName, features := range featureSources {
		sources[viewName] = &core.OnDemandSource{
			Source: &core.OnDemandSource_FeatureViewProjection{
				FeatureViewProjection: &core.FeatureViewProjection{
					FeatureViewName: viewName,
					FeatureColumns:  features,
					JoinKeyMap:      map[string]string{},
				},
			},
		}
	}

	proto := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:     name,
			Sources:  sources,
			Features: features,
		},
	}
	return model.NewOnDemandFeatureViewFromProto(proto)
}

func TestUnpackFeatureService(t *testing.T) {
	featASpec := createFeature("featA", types.ValueType_INT32)
	featBSpec := createFeature("featB", types.ValueType_INT32)
	featCSpec := createFeature("featC", types.ValueType_INT32)
	featDSpec := createFeature("featD", types.ValueType_INT32)
	featESpec := createFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := createFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := createFeature("featG", types.ValueType_FLOAT)
	featSSpec := createFeature("featS", types.ValueType_FLOAT)
	sortKeyA := createSortKey("featS", core.SortOrder_DESC, types.ValueType_FLOAT)

	viewA := createFeatureView("viewA", []string{"entity"}, featASpec, featBSpec)
	viewB := createFeatureView("viewB", []string{"entity"}, featCSpec, featDSpec)
	viewC := createFeatureView("viewC", []string{"entity"}, featESpec)
	viewS := createSortedFeatureView("viewS", []string{"entity"}, []*core.SortKey{sortKeyA}, featSSpec)
	onDemandView := createOnDemandFeatureView(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)

	fs := createFeatureService(map[string][]*core.FeatureSpecV2{
		"viewA": {featASpec, featBSpec},
		"viewB": {featCSpec},
		"odfv":  {onDemandFeature2},
		"viewS": {featSSpec},
	})

	fvs, sortedFvs, odfvs, err := GetFeatureViewsToUseByService(
		fs,
		map[string]*model.FeatureView{"viewA": viewA, "viewB": viewB, "viewC": viewC},
		map[string]*model.SortedFeatureView{"viewS": viewS},
		map[string]*model.OnDemandFeatureView{"odfv": onDemandView})

	assertCorrectUnpacking(t, fvs, sortedFvs, odfvs, err)
}

func assertCorrectUnpacking(t *testing.T, fvs []*FeatureViewAndRefs, sortedFvs []*SortedFeatureViewAndRefs, odfvs []*model.OnDemandFeatureView, err error) {
	assert.Nil(t, err)
	assert.Len(t, fvs, 3)
	assert.Len(t, sortedFvs, 1)
	assert.Len(t, odfvs, 1)

	fvsByName := make(map[string]*FeatureViewAndRefs)
	for _, fv := range fvs {
		fvsByName[fv.View.Base.Name] = fv
	}

	// feature views and features as declared in service
	assert.Equal(t, []string{"featA", "featB"}, fvsByName["viewA"].FeatureRefs)
	assert.Equal(t, []string{"featC"}, fvsByName["viewB"].FeatureRefs)

	// dependency of the on demand feature view
	assert.Equal(t, []string{"featE"}, fvsByName["viewC"].FeatureRefs)

	// only requested features projected
	assert.Len(t, odfvs[0].Base.Projection.Features, 1)
	assert.Equal(t, "featG", odfvs[0].Base.Projection.Features[0].Name)

	// sorted feature views and features as declared in service
	assert.Equal(t, []string{"featS"}, sortedFvs[0].FeatureRefs)
}

func TestUnpackFeatureViewsByReferences(t *testing.T) {
	featASpec := createFeature("featA", types.ValueType_INT32)
	featBSpec := createFeature("featB", types.ValueType_INT32)
	featCSpec := createFeature("featC", types.ValueType_INT32)
	featDSpec := createFeature("featD", types.ValueType_INT32)
	featESpec := createFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := createFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := createFeature("featG", types.ValueType_FLOAT)
	featSSpec := createFeature("featS", types.ValueType_FLOAT)
	sortKeyA := createSortKey("featS", core.SortOrder_DESC, types.ValueType_FLOAT)

	viewA := createFeatureView("viewA", []string{"entity"}, featASpec, featBSpec)
	viewB := createFeatureView("viewB", []string{"entity"}, featCSpec, featDSpec)
	viewC := createFeatureView("viewC", []string{"entity"}, featESpec)
	viewS := createSortedFeatureView("viewS", []string{"entity"}, []*core.SortKey{sortKeyA}, featSSpec)
	onDemandView := createOnDemandFeatureView(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)

	fvs, sortedFvs, odfvs, err := GetFeatureViewsToUseByFeatureRefs(
		[]string{
			"viewA:featA",
			"viewA:featB",
			"viewB:featC",
			"odfv:featG",
			"viewS:featS",
		},
		map[string]*model.FeatureView{"viewA": viewA, "viewB": viewB, "viewC": viewC},
		map[string]*model.SortedFeatureView{"viewS": viewS},
		map[string]*model.OnDemandFeatureView{"odfv": onDemandView})

	assertCorrectUnpacking(t, fvs, sortedFvs, odfvs, err)
}

func TestValidateSortKeyFilters_ValidFilters(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := createSortKey("name", core.SortOrder_ASC, types.ValueType_STRING)

	sfv1 := createSortedFeatureView("sfv1", []string{"driver"},
		[]*core.SortKey{sortKey1, sortKey2},
		createFeature("f1", types.ValueType_DOUBLE))

	sfv2 := createSortedFeatureView("sfv2", []string{"customer"},
		[]*core.SortKey{sortKey3},
		createFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	validFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart:     &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
					RangeEnd:       &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 50.0}},
					StartInclusive: true,
					EndInclusive:   true,
				},
			},
		},
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
	}

	err := ValidateSortKeyFilters(validFilters, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")

	sfv3 := createSortedFeatureView("sfv2", []string{"customer"},
		[]*core.SortKey{sortKey1, sortKey3, sortKey2},
		createFeature("f3", types.ValueType_STRING))

	sortedViews = []*SortedFeatureViewAndRefs{
		{View: sfv3, FeatureRefs: []string{"f3"}},
	}

	validFilters = []*serving.SortKeyFilter{
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart:     &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
					RangeEnd:       &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 50.0}},
					StartInclusive: true,
					EndInclusive:   true,
				},
			},
		},
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
		{
			SortKeyName: "name",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_StringVal{StringVal: "John"}},
			},
		},
	}

	err = ValidateSortKeyFilters(validFilters, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")
}

func TestValidateSortKeyFilters_NonExistentKey(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := createSortKey("name", core.SortOrder_ASC, types.ValueType_STRING)

	sfv1 := createSortedFeatureView("sfv1", []string{"driver"},
		[]*core.SortKey{sortKey1, sortKey2},
		createFeature("f1", types.ValueType_DOUBLE))

	sfv2 := createSortedFeatureView("sfv2", []string{"customer"},
		[]*core.SortKey{sortKey3},
		createFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	nonExistentKeyFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "non_existent_key",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_Int64Val{Int64Val: 123}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(nonExistentKeyFilter, sortedViews)
	assert.Error(t, err, "Non-existent sort key should produce an error")
	assert.Contains(t, err.Error(), "not found in any of the requested sorted feature views")
}

func TestValidateSortKeyFilters_TypeMismatch(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := createSortKey("name", core.SortOrder_ASC, types.ValueType_STRING)

	sfv1 := createSortedFeatureView("sfv1", []string{"driver"},
		[]*core.SortKey{sortKey1, sortKey2},
		createFeature("f1", types.ValueType_DOUBLE))

	sfv2 := createSortedFeatureView("sfv2", []string{"customer"},
		[]*core.SortKey{sortKey3},
		createFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	typeMismatchFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_StringVal{StringVal: "2022-01-01"}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(typeMismatchFilter, sortedViews)
	assert.Error(t, err, "Type mismatch should produce an error")
	assert.Contains(t, err.Error(), "has incompatible type")
}

func TestValidateSortKeyFilters_InvalidRangeFilter(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := createSortKey("name", core.SortOrder_ASC, types.ValueType_STRING)

	sfv1 := createSortedFeatureView("sfv1", []string{"driver"},
		[]*core.SortKey{sortKey1, sortKey2},
		createFeature("f1", types.ValueType_DOUBLE))

	sfv2 := createSortedFeatureView("sfv2", []string{"customer"},
		[]*core.SortKey{sortKey1, sortKey2, sortKey3},
		createFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	invalidRangeFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
				},
			},
		},
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.5}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(invalidRangeFilter, sortedViews)
	assert.Error(t, err, "Only the last sort key filter may have a range query")
	assert.Contains(t, err.Error(), "sort key filter for sort key 'timestamp' must have query type equals instead of range")

	invalidRangeFilter = []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1}},
			},
		},
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeEnd:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.5}},
					EndInclusive: true,
				},
			},
		},
		{
			SortKeyName: "name",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_StringVal{StringVal: "A"}},
				},
			},
		},
	}

	err = ValidateSortKeyFilters(invalidRangeFilter, sortedViews)
	assert.Error(t, err, "Sort key filter must have equality relations for all sort keys except the last one")
	assert.Contains(t, err.Error(), "sort key filter for sort key 'price' must have query type equals instead of range")
}

func TestValidateSortKeyFilters_InvalidEqualsFilter(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("price", core.SortOrder_ASC, types.ValueType_DOUBLE)

	sfv1 := createSortedFeatureView("sfv1", []string{"driver"},
		[]*core.SortKey{sortKey1, sortKey2},
		createFeature("f1", types.ValueType_DOUBLE))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
	}

	invalidRangeFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
			},
		},
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.5}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(invalidRangeFilter, sortedViews)
	assert.Error(t, err, "Sort key filter equality value cannot be null")
	assert.Contains(t, err.Error(), "equals value for sort key 'timestamp' has incompatible type: expected UNIX_TIMESTAMP")
}

func TestValidateSortKeyFilters_MissingFilter(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := createSortKey("name", core.SortOrder_ASC, types.ValueType_STRING)

	sfv1 := createSortedFeatureView("sfv1", []string{"driver"},
		[]*core.SortKey{sortKey1, sortKey2, sortKey3},
		createFeature("f1", types.ValueType_DOUBLE))

	sfv2 := createSortedFeatureView("sfv2", []string{"customer"},
		[]*core.SortKey{sortKey3},
		createFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	missingFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
		{
			SortKeyName: "name",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_StringVal{StringVal: "A"}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(missingFilters, sortedViews)
	assert.Error(t, err, "Must include all previous sort keys in the filter list")
	assert.Contains(t, err.Error(), "specify sort key filter in request for sort key: 'price' with query type equals")
}

func TestGroupSortedFeatureRefs(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("featureF", core.SortOrder_ASC, types.ValueType_DOUBLE)
	viewA := createSortedFeatureView("viewA", []string{"driver", "customer"},
		[]*core.SortKey{sortKey1},
		createFeature("featureA", types.ValueType_DOUBLE),
		createFeature("featureB", types.ValueType_DOUBLE))

	viewB := createSortedFeatureView("viewB", []string{"driver", "customer"},
		[]*core.SortKey{sortKey1},
		createFeature("featureC", types.ValueType_DOUBLE),
		createFeature("featureD", types.ValueType_DOUBLE))

	viewC := createSortedFeatureView("viewC", []string{"driver"},
		[]*core.SortKey{sortKey1},
		createFeature("featureE", types.ValueType_DOUBLE))

	viewD := createSortedFeatureView("viewD", []string{"customer"},
		[]*core.SortKey{sortKey2},
		createFeature("featureF", types.ValueType_DOUBLE))

	if viewA.Base != nil && viewA.Base.Projection == nil {
		viewA.Base.Projection = &model.FeatureViewProjection{
			NameAlias: "aliasViewA",
		}
	}

	sortKeyFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
		{
			SortKeyName: "featureF",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeEnd:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 1.5}},
					EndInclusive: true,
				},
			},
		},
	}

	refGroups, err := GroupSortedFeatureRefs(
		[]*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
			{View: viewC, FeatureRefs: []string{"featureE"}},
			{View: viewD, FeatureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		sortKeyFilters,
		false,
		10,
		true,
	)

	t.Logf("GroupSortedFeatureRefs returned %d groups", len(refGroups))
	for i, group := range refGroups {
		t.Logf("Group %d:", i)
		t.Logf("  Features: %v", group.FeatureNames)
		t.Logf("  AliasedNames: %v", group.AliasedFeatureNames)
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, refGroups, "Should return at least one group")

	for _, group := range refGroups {
		assert.Equal(t, 1, len(group.SortKeyFilters))
		if group.SortKeyFilters[0].SortKeyName == "timestamp" {
			assert.Equal(t, sortKeyFilters[0].SortKeyName, group.SortKeyFilters[0].SortKeyName)
			assert.Equal(t, sortKeyFilters[0].GetEquals().GetUnixTimestampVal(), group.SortKeyFilters[0].Equals)
			assert.Nil(t, group.SortKeyFilters[0].RangeStart)
			assert.Nil(t, group.SortKeyFilters[0].RangeEnd)
			assert.Equal(t, "DESC", group.SortKeyFilters[0].Order.Order.String())
		} else {
			assert.Equal(t, sortKeyFilters[1].SortKeyName, group.SortKeyFilters[0].SortKeyName)
			assert.Equal(t, sortKeyFilters[1].GetRange().RangeEnd.GetDoubleVal(), group.SortKeyFilters[0].RangeEnd)
			assert.Equal(t, sortKeyFilters[1].GetRange().EndInclusive, group.SortKeyFilters[0].EndInclusive)
			assert.Nil(t, group.SortKeyFilters[0].RangeStart)
			assert.Nil(t, group.SortKeyFilters[0].Equals)
			assert.Equal(t, "ASC", group.SortKeyFilters[0].Order.Order.String())
		}
		assert.Equal(t, int32(10), group.Limit)
	}

	featureAFound := false
	featureCFound := false
	featureEFound := false

	for _, group := range refGroups {
		for _, feature := range group.FeatureNames {
			if feature == "featureA" {
				featureAFound = true
			}
			if feature == "featureC" {
				featureCFound = true
			}
			if feature == "featureE" {
				featureEFound = true
			}
		}
	}

	assert.True(t, featureAFound, "Feature A should be present in results")
	assert.True(t, featureCFound, "Feature C should be present in results")
	assert.True(t, featureEFound, "Feature E should be present in results")
}

func TestGroupSortedFeatureRefs_withReverseSortOrder(t *testing.T) {
	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := createSortKey("featureB", core.SortOrder_ASC, types.ValueType_DOUBLE)
	viewA := createSortedFeatureView("viewA", []string{"driver", "customer"},
		[]*core.SortKey{sortKey1, sortKey2},
		createFeature("featureA", types.ValueType_DOUBLE),
		createFeature("featureB", types.ValueType_DOUBLE))

	sortKeyFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart:     &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
					RangeEnd:       &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1672531200}},
					StartInclusive: true,
					EndInclusive:   false,
				},
			},
		},
	}

	refGroups, err := GroupSortedFeatureRefs(
		[]*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		sortKeyFilters,
		true,
		10,
		true,
	)

	t.Logf("GroupSortedFeatureRefs returned %d groups", len(refGroups))
	for i, group := range refGroups {
		t.Logf("Group %d:", i)
		t.Logf("  Features: %v", group.FeatureNames)
		t.Logf("  AliasedNames: %v", group.AliasedFeatureNames)
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, refGroups, "Should return at least one group")

	for _, group := range refGroups {
		assert.Equal(t, 2, len(group.SortKeyFilters))
		assert.Equal(t, sortKeyFilters[0].SortKeyName, group.SortKeyFilters[0].SortKeyName)
		assert.Equal(t, sortKeyFilters[0].GetRange().RangeStart.GetUnixTimestampVal(), group.SortKeyFilters[0].RangeStart)
		assert.Equal(t, sortKeyFilters[0].GetRange().RangeEnd.GetUnixTimestampVal(), group.SortKeyFilters[0].RangeEnd)
		assert.Equal(t, sortKeyFilters[0].GetRange().StartInclusive, group.SortKeyFilters[0].StartInclusive)
		assert.Equal(t, sortKeyFilters[0].GetRange().EndInclusive, group.SortKeyFilters[0].EndInclusive)
		assert.Equal(t, "ASC", group.SortKeyFilters[0].Order.Order.String())

		// SortKeys missing from the filters should have a default filter with only Order assigned
		assert.Equal(t, sortKey2.Name, group.SortKeyFilters[1].SortKeyName)
		assert.Nil(t, group.SortKeyFilters[1].RangeStart)
		assert.Nil(t, group.SortKeyFilters[1].RangeEnd)
		assert.Equal(t, "DESC", group.SortKeyFilters[1].Order.Order.String())

		assert.Equal(t, int32(10), group.Limit)
	}

	featureAFound := false

	for _, group := range refGroups {
		for _, feature := range group.FeatureNames {
			if feature == "featureA" {
				featureAFound = true
			}
		}
	}

	assert.True(t, featureAFound, "Feature A should be present in results")
}

func TestEntitiesToRangeFeatureVectors(t *testing.T) {
	entityColumns := map[string]*types.RepeatedValue{
		"driver_id": {Val: []*types.Value{
			{Val: &types.Value_Int32Val{Int32Val: 1}},
			{Val: &types.Value_Int32Val{Int32Val: 2}},
			{Val: &types.Value_Int32Val{Int32Val: 3}},
		}},
		"customer_id": {Val: []*types.Value{
			{Val: &types.Value_StringVal{StringVal: "A"}},
			{Val: &types.Value_StringVal{StringVal: "B"}},
			{Val: &types.Value_StringVal{StringVal: "C"}},
		}},
	}

	arrowAllocator := memory.NewGoAllocator()
	numRows := 3

	vectors, err := EntitiesToRangeFeatureVectors(entityColumns, arrowAllocator, numRows)

	assert.NoError(t, err)
	assert.Len(t, vectors, 2)

	var driverVector, customerVector *RangeFeatureVector
	for _, vector := range vectors {
		if vector.Name == "driver_id" {
			driverVector = vector
		} else if vector.Name == "customer_id" {
			customerVector = vector
		}
	}

	require.NotNil(t, driverVector)
	assert.Equal(t, "driver_id", driverVector.Name)
	assert.Len(t, driverVector.RangeStatuses, numRows)
	assert.Len(t, driverVector.RangeTimestamps, numRows)

	for i := 0; i < numRows; i++ {
		assert.Len(t, driverVector.RangeStatuses[i], 1)
		assert.Equal(t, serving.FieldStatus_PRESENT, driverVector.RangeStatuses[i][0])
		assert.Len(t, driverVector.RangeTimestamps[i], 1)
	}

	require.NotNil(t, customerVector)
	assert.Equal(t, "customer_id", customerVector.Name)
	assert.Len(t, customerVector.RangeStatuses, numRows)
	assert.Len(t, customerVector.RangeTimestamps, numRows)

	assert.NotNil(t, driverVector.RangeValues)
	assert.NotNil(t, customerVector.RangeValues)

	driverVector.RangeValues.Release()
	customerVector.RangeValues.Release()
}

func TestTransposeRangeFeatureRowsIntoColumns(t *testing.T) {
	arrowAllocator := memory.NewGoAllocator()
	numRows := 2

	sortKey1 := createSortKey("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sfv := createSortedFeatureView("testView", []string{"driver"}, []*core.SortKey{sortKey1},
		createFeature("f1", types.ValueType_DOUBLE))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv, FeatureRefs: []string{"f1"}},
	}

	groupRef := &GroupedRangeFeatureRefs{
		FeatureNames:        []string{"f1"},
		FeatureViewNames:    []string{"testView"},
		AliasedFeatureNames: []string{"testView__f1"},
		Indices:             [][]int{{0}, {1}},
	}

	nowTime := time.Now()
	yesterdayTime := nowTime.Add(-24 * time.Hour)

	featureData := [][]onlinestore.RangeFeatureData{
		{
			{
				FeatureView: "testView",
				FeatureName: "f1",
				Values:      []interface{}{42.5, 43.2},
				EventTimestamps: []timestamp.Timestamp{
					{Seconds: nowTime.Unix()},
					{Seconds: yesterdayTime.Unix()},
				},
			},
		},
		{
			{
				FeatureView: "testView",
				FeatureName: "f1",
				Values:      []interface{}{99.9},
				EventTimestamps: []timestamp.Timestamp{
					{Seconds: nowTime.Unix()},
				},
			},
		},
	}

	vectors, err := TransposeRangeFeatureRowsIntoColumns(featureData, groupRef, sortedViews, arrowAllocator, numRows)

	assert.NoError(t, err)
	assert.Len(t, vectors, 1)
	vector := vectors[0]
	assert.Equal(t, "testView__f1", vector.Name)
	assert.Len(t, vector.RangeStatuses, numRows)
	assert.Len(t, vector.RangeTimestamps, numRows)
	assert.Len(t, vector.RangeStatuses[0], 2)
	assert.Len(t, vector.RangeTimestamps[0], 2)
	assert.Equal(t, serving.FieldStatus_PRESENT, vector.RangeStatuses[0][0])
	assert.Len(t, vector.RangeStatuses[1], 1)
	assert.Len(t, vector.RangeTimestamps[1], 1)
	assert.Equal(t, serving.FieldStatus_PRESENT, vector.RangeStatuses[1][0])
	assert.NotNil(t, vector.RangeValues)
	vector.RangeValues.Release()
}

func TestValidateFeatureRefs(t *testing.T) {
	t.Run("NoCollisions", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{
				Name: "viewA",
				Projection: &model.FeatureViewProjection{
					NameAlias: "aliasViewA",
				},
			},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "No collisions should result in no error")
	})

	t.Run("NoCollisionsWithFullFeatureNames", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{
				Name: "viewA",
				Projection: &model.FeatureViewProjection{
					NameAlias: "aliasViewA",
				},
			},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "Collisions with full feature names should not result in an error")
	})

	t.Run("CollisionsWithoutFullFeatureNames", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{
				Name: "viewA",
				Projection: &model.FeatureViewProjection{
					NameAlias: "aliasViewA",
				},
			},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, false)
		assert.Error(t, err, "Collisions without full feature names should result in an error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
	})

	t.Run("SingleFeatureNoCollision", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewA"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "Single feature with no collision should not result in an error")
	})

	t.Run("EmptyFeatureRefs", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewA"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "Empty feature references should not result in an error")
	})

	t.Run("MultipleCollisions", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewA"},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureB"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, false)
		assert.Error(t, err, "Multiple collisions should result in an error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
		assert.Contains(t, err.Error(), "featureB", "Error should include the collided feature name")
	})
}
func TestValidateSortedFeatureRefs(t *testing.T) {
	t.Run("NoCollisions", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "viewA",
					Projection: &model.FeatureViewProjection{
						NameAlias: "aliasViewA",
					},
				},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "No collisions should result in no error")
	})

	t.Run("NoCollisionsWithFullFeatureNames", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "viewA",
					Projection: &model.FeatureViewProjection{
						NameAlias: "aliasViewA",
					},
				},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "Collisions with full feature names should not result in an error")
	})

	t.Run("CollisionsWithoutFullFeatureNames", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "viewA",
					Projection: &model.FeatureViewProjection{
						NameAlias: "aliasViewA",
					},
				},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, false)
		assert.Error(t, err, "Collisions without full feature names should result in an error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
	})

	t.Run("SingleFeatureNoCollision", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewA"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "Single feature with no collision should not result in an error")
	})

	t.Run("EmptyFeatureRefs", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewA"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "Empty feature references should not result in an error")
	})

	t.Run("MultipleCollisions", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewA"},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureB"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, false)
		assert.Error(t, err, "Multiple collisions should result in an error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
		assert.Contains(t, err.Error(), "featureB", "Error should include the collided feature name")
	})
}
func BenchmarkValidateFeatureRefs(b *testing.B) {
	// Prepare mock data for the benchmark
	requestedFeatures := generateMockFeatureViewAndRefs(10, 100)
	fullFeatureNames := true

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := ValidateFeatureRefs(requestedFeatures, fullFeatureNames)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// Helper function to generate mock FeatureViewAndRefs
func generateMockFeatureViewAndRefs(numViews, numFeatures int) []*FeatureViewAndRefs {
	featureViews := make([]*FeatureViewAndRefs, numViews)
	for i := 0; i < numViews; i++ {
		features := make([]string, numFeatures)
		for j := 0; j < numFeatures; j++ {
			features[j] = fmt.Sprintf("feature_%d", j)
		}
		featureViews[i] = &FeatureViewAndRefs{
			View: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: fmt.Sprintf("view_%d", i),
				},
			},
			FeatureRefs: features,
		}
	}
	return featureViews
}

func BenchmarkTransposeFeatureRowsIntoColumns(b *testing.B) {
	// Mock Data
	numRows := 1000
	numFeatures := 100

	featureData2D := make([][]onlinestore.FeatureData, numRows)
	for i := 0; i < numRows; i++ {
		featureData2D[i] = make([]onlinestore.FeatureData, numFeatures)
		for j := 0; j < numFeatures; j++ {
			featureData2D[i][j] = onlinestore.FeatureData{
				Value: types.Value{Val: &types.Value_Int64Val{Int64Val: int64(i * j)}},
				Timestamp: timestamppb.Timestamp{
					Seconds: int64(i * j),
				},
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: "feature_view",
					FeatureName:     "feature_" + strconv.Itoa(j),
				},
			}
		}
	}

	groupRef := &GroupedFeaturesPerEntitySet{
		AliasedFeatureNames: make([]string, numFeatures),
		Indices:             make([][]int, numRows),
	}
	for i := 0; i < numFeatures; i++ {
		groupRef.AliasedFeatureNames[i] = "feature_" + strconv.Itoa(i)
	}
	for i := 0; i < numRows; i++ {
		groupRef.Indices[i] = []int{i}
	}

	requestedFeatureViews := []*FeatureViewAndRefs{
		{
			View: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "feature_view",
				},
				Ttl: &durationpb.Duration{Seconds: 0, Nanos: 0},
			},
		},
	}

	arrowAllocator := memory.NewGoAllocator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := TransposeFeatureRowsIntoColumns(featureData2D, groupRef, requestedFeatureViews, arrowAllocator, numRows)
		if err != nil {
			b.Fatalf("Error during TransposeFeatureRowsIntoColumns: %v", err)
		}
	}
}
