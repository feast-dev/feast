package logging

import (
	"fmt"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type FeatureServiceSchema struct {
	JoinKeys    []string
	Features    []string
	RequestData []string

	JoinKeysTypes    map[string]types.ValueType_Enum
	FeaturesTypes    map[string]types.ValueType_Enum
	RequestDataTypes map[string]types.ValueType_Enum
}

func GenerateSchemaFromFeatureService(fs FeatureStore, featureServiceName string) (*FeatureServiceSchema, error) {
	entityMap, fvMap, odFvMap, err := fs.GetFcosMap()
	if err != nil {
		return nil, err
	}

	featureService, err := fs.GetFeatureService(featureServiceName)
	if err != nil {
		return nil, err
	}

	return generateSchema(featureService, entityMap, fvMap, odFvMap)
}

func generateSchema(featureService *model.FeatureService, entityMap map[string]*model.Entity, fvMap map[string]*model.FeatureView, odFvMap map[string]*model.OnDemandFeatureView) (*FeatureServiceSchema, error) {
	joinKeys := make([]string, 0)
	features := make([]string, 0)
	requestData := make([]string, 0)

	joinKeysSet := make(map[string]interface{})

	entityJoinKeyToType := make(map[string]types.ValueType_Enum)
	allFeatureTypes := make(map[string]types.ValueType_Enum)
	requestDataTypes := make(map[string]types.ValueType_Enum)

	for _, featureProjection := range featureService.Projections {
		// Create copies of FeatureView that may contains the same *FeatureView but
		// each differentiated by a *FeatureViewProjection
		featureViewName := featureProjection.Name
		if fv, ok := fvMap[featureViewName]; ok {
			for _, f := range featureProjection.Features {
				fullFeatureName := getFullFeatureName(featureProjection.NameToUse(), f.Name)
				features = append(features, fullFeatureName)
				allFeatureTypes[fullFeatureName] = f.Dtype
			}
			for _, entityColumn := range fv.EntityColumns {
				var joinKey string
				if joinKeyAlias, ok := featureProjection.JoinKeyMap[entityColumn.Name]; ok {
					joinKey = joinKeyAlias
				} else {
					joinKey = entityColumn.Name
				}

				if _, ok := joinKeysSet[joinKey]; !ok {
					joinKeys = append(joinKeys, joinKey)
				}

				joinKeysSet[joinKey] = nil
				entityJoinKeyToType[joinKey] = entityColumn.Dtype
			}
		} else if odFv, ok := odFvMap[featureViewName]; ok {
			for _, f := range featureProjection.Features {
				fullFeatureName := getFullFeatureName(featureProjection.NameToUse(), f.Name)
				features = append(features, fullFeatureName)
				allFeatureTypes[fullFeatureName] = f.Dtype
			}
			for paramName, paramType := range odFv.GetRequestDataSchema() {
				requestData = append(requestData, paramName)
				requestDataTypes[paramName] = paramType
			}
		} else {
			return nil, fmt.Errorf("no such feature view %s found (referenced from feature service %s)",
				featureViewName, featureService.Name)
		}
	}

	schema := &FeatureServiceSchema{
		JoinKeys:    joinKeys,
		Features:    features,
		RequestData: requestData,

		JoinKeysTypes:    entityJoinKeyToType,
		FeaturesTypes:    allFeatureTypes,
		RequestDataTypes: requestDataTypes,
	}
	return schema, nil
}
