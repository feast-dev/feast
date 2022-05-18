package logging

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func buildFCOMaps(entities []*model.Entity, fvs []*model.FeatureView, odFvs []*model.OnDemandFeatureView) (map[string]*model.Entity, map[string]*model.FeatureView, map[string]*model.OnDemandFeatureView) {
	entityMap := make(map[string]*model.Entity)
	fvMap := make(map[string]*model.FeatureView)
	odFvMap := make(map[string]*model.OnDemandFeatureView)

	for _, entity := range entities {
		entityMap[entity.Name] = entity
	}

	for _, fv := range fvs {
		fvMap[fv.Base.Name] = fv
	}

	for _, fv := range odFvs {
		odFvMap[fv.Base.Name] = fv
	}

	return entityMap, fvMap, odFvMap
}

func TestSchemaTypeRetrieval(t *testing.T) {
	featureService, entities, fvs, odfvs := InitializeFeatureRepoVariablesForTest()
	entityMap, fvMap, odFvMap := buildFCOMaps(entities, fvs, odfvs)

	expectedFeatureNames := make([]string, 0)
	expectedRequestData := make([]string, 0)

	for _, featureView := range fvs {
		for _, f := range featureView.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, getFullFeatureName(featureView.Base.Name, f.Name))
		}
	}
	for _, odFv := range odfvs {
		for _, f := range odFv.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, getFullFeatureName(odFv.Base.Name, f.Name))
		}
		for _, dataSource := range odFv.SourceRequestDataSources {
			for _, field := range dataSource.Schema {
				expectedRequestData = append(expectedRequestData, field.Name)
			}
		}
	}

	schema, err := generateSchema(featureService, entityMap, fvMap, odFvMap)
	assert.Nil(t, err)

	assert.Equal(t, expectedFeatureNames, schema.Features)
	assert.Equal(t, []string{"driver_id"}, schema.JoinKeys)
	assert.Equal(t, schema.JoinKeysTypes["driver_id"], types.ValueType_INT64)

	types := []types.ValueType_Enum{*types.ValueType_INT64.Enum(), *types.ValueType_FLOAT.Enum(), *types.ValueType_INT32.Enum(), *types.ValueType_DOUBLE.Enum(), *types.ValueType_INT32.Enum(), *types.ValueType_DOUBLE.Enum()}
	for idx, featureName := range expectedFeatureNames {
		assert.Contains(t, schema.FeaturesTypes, featureName)
		assert.Equal(t, schema.FeaturesTypes[featureName], types[idx])
	}
}

func TestSchemaRetrievalIgnoresEntitiesNotInFeatureService(t *testing.T) {
	featureService, entities, fvs, odfvs := InitializeFeatureRepoVariablesForTest()
	entityMap, fvMap, odFvMap := buildFCOMaps(entities, fvs, odfvs)

	// Remove entities in featureservice
	for _, featureView := range fvs {
		featureView.EntityNames = []string{}
		featureView.EntityColumns = []*model.Field{}
	}

	schema, err := generateSchema(featureService, entityMap, fvMap, odFvMap)
	assert.Nil(t, err)
	assert.Empty(t, schema.JoinKeysTypes)
}

func TestSchemaUsesOrderInFeatureService(t *testing.T) {
	featureService, entities, fvs, odfvs := InitializeFeatureRepoVariablesForTest()
	entityMap, fvMap, odFvMap := buildFCOMaps(entities, fvs, odfvs)

	expectedFeatureNames := make([]string, 0)

	// Source of truth for order of featureNames
	for _, featureView := range fvs {
		for _, f := range featureView.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, getFullFeatureName(featureView.Base.Name, f.Name))
		}
	}
	for _, featureView := range odfvs {
		for _, f := range featureView.Base.Features {
			expectedFeatureNames = append(expectedFeatureNames, getFullFeatureName(featureView.Base.Name, f.Name))
		}
	}

	rand.Seed(time.Now().UnixNano())
	// Shuffle the featureNames in incorrect order
	for _, featureView := range fvs {
		rand.Shuffle(len(featureView.Base.Features), func(i, j int) {
			featureView.Base.Features[i], featureView.Base.Features[j] = featureView.Base.Features[j], featureView.Base.Features[i]
		})
	}
	for _, featureView := range odfvs {
		rand.Shuffle(len(featureView.Base.Features), func(i, j int) {
			featureView.Base.Features[i], featureView.Base.Features[j] = featureView.Base.Features[j], featureView.Base.Features[i]
		})
	}

	schema, err := generateSchema(featureService, entityMap, fvMap, odFvMap)
	assert.Nil(t, err)

	// Ensure the same results
	assert.Equal(t, expectedFeatureNames, schema.Features)
	assert.Equal(t, []string{"driver_id"}, schema.JoinKeys)

}

// Initialize all dummy featureservice, entities and featureviews/on demand featureviews for testing.
func InitializeFeatureRepoVariablesForTest() (*model.FeatureService, []*model.Entity, []*model.FeatureView, []*model.OnDemandFeatureView) {
	f1 := test.CreateNewField(
		"int64",
		types.ValueType_INT64,
	)
	f2 := test.CreateNewField(
		"float32",
		types.ValueType_FLOAT,
	)
	projection1 := test.CreateNewFeatureViewProjection(
		"featureView1",
		"",
		[]*model.Field{f1, f2},
		map[string]string{},
	)
	baseFeatureView1 := test.CreateBaseFeatureView(
		"featureView1",
		[]*model.Field{f1, f2},
		projection1,
	)
	entity1 := test.CreateNewEntity("driver_id", "driver_id")
	entitycolumn1 := test.CreateNewField(
		"driver_id",
		types.ValueType_INT64,
	)
	featureView1 := test.CreateFeatureView(baseFeatureView1, nil, []string{"driver_id"}, []*model.Field{entitycolumn1})
	f3 := test.CreateNewField(
		"int32",
		types.ValueType_INT32,
	)
	f4 := test.CreateNewField(
		"double",
		types.ValueType_DOUBLE,
	)
	projection2 := test.CreateNewFeatureViewProjection(
		"featureView2",
		"",
		[]*model.Field{f3, f4},
		map[string]string{},
	)
	baseFeatureView2 := test.CreateBaseFeatureView(
		"featureView2",
		[]*model.Field{f3, f4},
		projection2,
	)
	featureView2 := test.CreateFeatureView(baseFeatureView2, nil, []string{"driver_id"}, []*model.Field{entitycolumn1})

	f5 := test.CreateNewField(
		"odfv_f1",
		types.ValueType_INT32,
	)
	f6 := test.CreateNewField(
		"odfv_f2",
		types.ValueType_DOUBLE,
	)
	projection3 := test.CreateNewFeatureViewProjection(
		"od_bf1",
		"",
		[]*model.Field{f5, f6},
		map[string]string{},
	)
	od_bf1 := test.CreateBaseFeatureView(
		"od_bf1",
		[]*model.Field{f5, f6},
		projection3,
	)
	odfv := model.NewOnDemandFeatureViewFromBase(od_bf1)
	odfv.SourceRequestDataSources["input"] = &core.DataSource_RequestDataOptions{
		Schema: []*core.FeatureSpecV2{
			{Name: "param1", ValueType: types.ValueType_FLOAT},
		},
	}
	featureService := test.CreateNewFeatureService(
		"test_service",
		"test_project",
		nil,
		nil,
		[]*model.FeatureViewProjection{projection1, projection2, projection3},
	)
	return featureService, []*model.Entity{entity1}, []*model.FeatureView{featureView1, featureView2}, []*model.OnDemandFeatureView{odfv}
}
