package onlineserving

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
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
	assert.Errorf(t, err, "key destination_id is missing in provided entity rows")
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

	viewA := createFeatureView("viewA", []string{"entity"}, featASpec, featBSpec)
	viewB := createFeatureView("viewB", []string{"entity"}, featCSpec, featDSpec)
	viewC := createFeatureView("viewC", []string{"entity"}, featESpec)
	onDemandView := createOnDemandFeatureView(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)

	fs := createFeatureService(map[string][]*core.FeatureSpecV2{
		"viewA": {featASpec, featBSpec},
		"viewB": {featCSpec},
		"odfv":  {onDemandFeature2},
	})

	fvs, odfvs, err := GetFeatureViewsToUseByService(
		fs,
		map[string]*model.FeatureView{"viewA": viewA, "viewB": viewB, "viewC": viewC},
		map[string]*model.OnDemandFeatureView{"odfv": onDemandView})

	assertCorrectUnpacking(t, fvs, odfvs, err)
}

func assertCorrectUnpacking(t *testing.T, fvs []*FeatureViewAndRefs, odfvs []*model.OnDemandFeatureView, err error) {
	assert.Nil(t, err)
	assert.Len(t, fvs, 3)
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
}

func TestUnpackFeatureViewsByReferences(t *testing.T) {
	featASpec := createFeature("featA", types.ValueType_INT32)
	featBSpec := createFeature("featB", types.ValueType_INT32)
	featCSpec := createFeature("featC", types.ValueType_INT32)
	featDSpec := createFeature("featD", types.ValueType_INT32)
	featESpec := createFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := createFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := createFeature("featG", types.ValueType_FLOAT)

	viewA := createFeatureView("viewA", []string{"entity"}, featASpec, featBSpec)
	viewB := createFeatureView("viewB", []string{"entity"}, featCSpec, featDSpec)
	viewC := createFeatureView("viewC", []string{"entity"}, featESpec)
	onDemandView := createOnDemandFeatureView(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)

	fvs, odfvs, err := GetFeatureViewsToUseByFeatureRefs(
		[]string{
			"viewA:featA",
			"viewA:featB",
			"viewB:featC",
			"odfv:featG",
		},
		map[string]*model.FeatureView{"viewA": viewA, "viewB": viewB, "viewC": viewC},
		map[string]*model.OnDemandFeatureView{"odfv": onDemandView})

	assertCorrectUnpacking(t, fvs, odfvs, err)
}

func defaultValueTestView(defaultValue *types.Value) *model.FeatureView {
	return &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "driver_stats",
			Features: []*model.Field{
				{Name: "conv_rate", Dtype: types.ValueType_INT64, DefaultValue: defaultValue},
			},
			Projection: &model.FeatureViewProjection{Name: "driver_stats"},
		},
		Ttl: durationpb.New(time.Hour * 24 * 365 * 100),
	}
}

func defaultValueTestGroupRef() *GroupedFeaturesPerEntitySet {
	return &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"conv_rate"},
		FeatureViewNames:    []string{"driver_stats"},
		AliasedFeatureNames: []string{"driver_stats__conv_rate"},
		Indices:             [][]int{{0}},
	}
}

func transposeSingle(t *testing.T, view *model.FeatureView, featureData2D [][]onlinestore.FeatureData) *FeatureVector {
	t.Helper()
	vectors, err := TransposeFeatureRowsIntoColumns(
		featureData2D,
		defaultValueTestGroupRef(),
		[]*FeatureViewAndRefs{{View: view, FeatureRefs: []string{"conv_rate"}}},
		memory.NewGoAllocator(),
		1,
	)
	assert.Nil(t, err)
	assert.Len(t, vectors, 1)
	return vectors[0]
}

func TestTransposeSubstitutesDefaultForMissingRow(t *testing.T) {
	view := defaultValueTestView(&types.Value{Val: &types.Value_Int64Val{Int64Val: 7}})

	vector := transposeSingle(t, view, [][]onlinestore.FeatureData{nil})

	assert.Equal(t, serving.FieldStatus_NOT_FOUND, vector.Statuses[0])
	assert.Equal(t, int64(7), vector.Values.(*array.Int64).Value(0))
}

func TestTransposeSubstitutesDefaultForNullValue(t *testing.T) {
	view := defaultValueTestView(&types.Value{Val: &types.Value_Int64Val{Int64Val: 7}})
	featureData2D := [][]onlinestore.FeatureData{{{
		Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_stats", FeatureName: "conv_rate"},
		Timestamp: timestamppb.Timestamp{Seconds: timestamppb.Now().Seconds},
		Value:     types.Value{Val: &types.Value_NullVal{}},
	}}}

	vector := transposeSingle(t, view, featureData2D)

	assert.Equal(t, serving.FieldStatus_NOT_FOUND, vector.Statuses[0])
	assert.Equal(t, int64(7), vector.Values.(*array.Int64).Value(0))
}

func TestTransposeLeavesPresentValueUntouched(t *testing.T) {
	view := defaultValueTestView(&types.Value{Val: &types.Value_Int64Val{Int64Val: 7}})
	featureData2D := [][]onlinestore.FeatureData{{{
		Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_stats", FeatureName: "conv_rate"},
		Timestamp: timestamppb.Timestamp{Seconds: timestamppb.Now().Seconds},
		Value:     types.Value{Val: &types.Value_Int64Val{Int64Val: 3}},
	}}}

	vector := transposeSingle(t, view, featureData2D)

	assert.Equal(t, serving.FieldStatus_PRESENT, vector.Statuses[0])
	assert.Equal(t, int64(3), vector.Values.(*array.Int64).Value(0))
}

func TestTransposeWithoutDefaultLeavesNull(t *testing.T) {
	view := defaultValueTestView(nil)

	vector := transposeSingle(t, view, [][]onlinestore.FeatureData{nil})

	assert.Equal(t, serving.FieldStatus_NOT_FOUND, vector.Statuses[0])
	// array.Null reports NullN rather than IsNull for this type.
	assert.Equal(t, 1, vector.Values.NullN())
}

// A default whose type differs from the stored values must not be inserted: the Arrow
// type comes from the first non-nil value, and a mismatch would serve real values as 0.
func TestTransposeDoesNotRetypeColumnWithMismatchedDefault(t *testing.T) {
	view := defaultValueTestView(&types.Value{Val: &types.Value_Int64Val{Int64Val: 7}})
	featureData2D := [][]onlinestore.FeatureData{
		nil, // missing row first, so the default would set the column type
		{{
			Reference: serving.FeatureReferenceV2{FeatureViewName: "driver_stats", FeatureName: "conv_rate"},
			Timestamp: timestamppb.Timestamp{Seconds: timestamppb.Now().Seconds},
			Value:     types.Value{Val: &types.Value_DoubleVal{DoubleVal: 2.5}},
		}},
	}

	vectors, err := TransposeFeatureRowsIntoColumns(
		featureData2D,
		&GroupedFeaturesPerEntitySet{
			FeatureNames:        []string{"conv_rate"},
			FeatureViewNames:    []string{"driver_stats"},
			AliasedFeatureNames: []string{"driver_stats__conv_rate"},
			Indices:             [][]int{{0}, {1}},
		},
		[]*FeatureViewAndRefs{{View: view, FeatureRefs: []string{"conv_rate"}}},
		memory.NewGoAllocator(),
		2,
	)
	assert.Nil(t, err)

	// The stored double must survive rather than being read back as 0.
	assert.Equal(t, 2.5, vectors[0].Values.(*array.Float64).Value(1))
}
