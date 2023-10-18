package onlineserving

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
)

/*
FeatureVector type represent result of retrieving single feature for multiple rows.
It can be imagined as a column in output dataframe / table.
It contains of feature name, list of values (across all rows),
list of statuses and list of timestamp. All these lists have equal length.
And this length is also equal to number of entity rows received in request.
*/
type FeatureVector struct {
	Name       string
	Values     arrow.Array
	Statuses   []serving.FieldStatus
	Timestamps []*timestamppb.Timestamp
}

type FeatureViewAndRefs struct {
	View        *model.FeatureView
	FeatureRefs []string
}

/*
We group all features from a single request by entities they attached to.
Thus, we will be able to call online retrieval per entity and not per each feature View.
In this struct we collect all features and views that belongs to a group.
We also store here projected entity keys (only ones that needed to retrieve these features)
and indexes to map result of retrieval into output response.
*/
type GroupedFeaturesPerEntitySet struct {
	// A list of requested feature references of the form featureViewName:featureName that share this entity set
	FeatureNames     []string
	FeatureViewNames []string
	// full feature references as they supposed to appear in response
	AliasedFeatureNames []string
	// Entity set as a list of EntityKeys to pass to OnlineRead
	EntityKeys []*prototypes.EntityKey
	// Reversed mapping to project result of retrieval from storage to response
	Indices [][]int
}

/*
Return

	(1) requested feature views and features grouped per View
	(2) requested on demand feature views

existed in the registry
*/
func GetFeatureViewsToUseByService(
	featureService *model.FeatureService,
	featureViews map[string]*model.FeatureView,
	onDemandFeatureViews map[string]*model.OnDemandFeatureView) ([]*FeatureViewAndRefs, []*model.OnDemandFeatureView, error) {

	viewNameToViewAndRefs := make(map[string]*FeatureViewAndRefs)
	odFvsToUse := make([]*model.OnDemandFeatureView, 0)

	for _, featureProjection := range featureService.Projections {
		// Create copies of FeatureView that may contains the same *FeatureView but
		// each differentiated by a *FeatureViewProjection
		featureViewName := featureProjection.Name
		if fv, ok := featureViews[featureViewName]; ok {
			base, err := fv.Base.WithProjection(featureProjection)
			if err != nil {
				return nil, nil, err
			}
			if _, ok := viewNameToViewAndRefs[featureProjection.NameToUse()]; !ok {
				viewNameToViewAndRefs[featureProjection.NameToUse()] = &FeatureViewAndRefs{
					View:        fv.NewFeatureViewFromBase(base),
					FeatureRefs: []string{},
				}
			}

			for _, feature := range featureProjection.Features {
				viewNameToViewAndRefs[featureProjection.NameToUse()].FeatureRefs =
					addStringIfNotContains(viewNameToViewAndRefs[featureProjection.NameToUse()].FeatureRefs,
						feature.Name)
			}

		} else if odFv, ok := onDemandFeatureViews[featureViewName]; ok {
			projectedOdFv, err := odFv.NewWithProjection(featureProjection)
			if err != nil {
				return nil, nil, err
			}
			odFvsToUse = append(odFvsToUse, projectedOdFv)
			err = extractOdFvDependencies(
				projectedOdFv,
				featureViews,
				viewNameToViewAndRefs)
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, fmt.Errorf("the provided feature service %s contains a reference to a feature View"+
				"%s which doesn't exist, please make sure that you have created the feature View"+
				"%s and that you have registered it by running \"apply\"", featureService.Name, featureViewName, featureViewName)
		}
	}

	fvsToUse := make([]*FeatureViewAndRefs, 0)
	for _, viewAndRef := range viewNameToViewAndRefs {
		fvsToUse = append(fvsToUse, viewAndRef)
	}

	return fvsToUse, odFvsToUse, nil
}

/*
Return

	(1) requested feature views and features grouped per View
	(2) requested on demand feature views

existed in the registry
*/
func GetFeatureViewsToUseByFeatureRefs(
	features []string,
	featureViews map[string]*model.FeatureView,
	onDemandFeatureViews map[string]*model.OnDemandFeatureView) ([]*FeatureViewAndRefs, []*model.OnDemandFeatureView, error) {
	viewNameToViewAndRefs := make(map[string]*FeatureViewAndRefs)
	odFvToFeatures := make(map[string][]string)

	for _, featureRef := range features {
		featureViewName, featureName, err := ParseFeatureReference(featureRef)
		if err != nil {
			return nil, nil, err
		}
		if fv, ok := featureViews[featureViewName]; ok {
			if viewAndRef, ok := viewNameToViewAndRefs[fv.Base.Name]; ok {
				viewAndRef.FeatureRefs = addStringIfNotContains(viewAndRef.FeatureRefs, featureName)
			} else {
				viewNameToViewAndRefs[fv.Base.Name] = &FeatureViewAndRefs{
					View:        fv,
					FeatureRefs: []string{featureName},
				}
			}
		} else if odfv, ok := onDemandFeatureViews[featureViewName]; ok {
			if _, ok := odFvToFeatures[odfv.Base.Name]; !ok {
				odFvToFeatures[odfv.Base.Name] = []string{featureName}
			} else {
				odFvToFeatures[odfv.Base.Name] = append(
					odFvToFeatures[odfv.Base.Name], featureName)
			}
		} else {
			return nil, nil, fmt.Errorf("feature View %s doesn't exist, please make sure that you have created the"+
				" feature View %s and that you have registered it by running \"apply\"", featureViewName, featureViewName)
		}
	}

	odFvsToUse := make([]*model.OnDemandFeatureView, 0)

	for odFvName, featureNames := range odFvToFeatures {
		projectedOdFv, err := onDemandFeatureViews[odFvName].ProjectWithFeatures(featureNames)
		if err != nil {
			return nil, nil, err
		}

		err = extractOdFvDependencies(
			projectedOdFv,
			featureViews,
			viewNameToViewAndRefs)
		if err != nil {
			return nil, nil, err
		}
		odFvsToUse = append(odFvsToUse, projectedOdFv)
	}

	fvsToUse := make([]*FeatureViewAndRefs, 0)
	for _, viewAndRefs := range viewNameToViewAndRefs {
		fvsToUse = append(fvsToUse, viewAndRefs)
	}

	return fvsToUse, odFvsToUse, nil
}

func extractOdFvDependencies(
	odFv *model.OnDemandFeatureView,
	sourceFvs map[string]*model.FeatureView,
	requestedFeatures map[string]*FeatureViewAndRefs,
) error {

	for _, sourceFvProjection := range odFv.SourceFeatureViewProjections {
		fv := sourceFvs[sourceFvProjection.Name]
		base, err := fv.Base.WithProjection(sourceFvProjection)
		if err != nil {
			return err
		}
		newFv := fv.NewFeatureViewFromBase(base)

		if _, ok := requestedFeatures[sourceFvProjection.NameToUse()]; !ok {
			requestedFeatures[sourceFvProjection.NameToUse()] = &FeatureViewAndRefs{
				View:        newFv,
				FeatureRefs: []string{},
			}
		}

		for _, feature := range sourceFvProjection.Features {
			requestedFeatures[sourceFvProjection.NameToUse()].FeatureRefs = addStringIfNotContains(
				requestedFeatures[sourceFvProjection.NameToUse()].FeatureRefs, feature.Name)
		}
	}

	return nil
}

func addStringIfNotContains(slice []string, element string) []string {
	found := false
	for _, item := range slice {
		if element == item {
			found = true
		}
	}
	if !found {
		slice = append(slice, element)
	}
	return slice
}

func GetEntityMaps(requestedFeatureViews []*FeatureViewAndRefs, entities []*model.Entity) (map[string]string, map[string]interface{}, error) {
	entityNameToJoinKeyMap := make(map[string]string)
	expectedJoinKeysSet := make(map[string]interface{})

	entitiesByName := make(map[string]*model.Entity)

	for _, entity := range entities {
		entitiesByName[entity.Name] = entity
	}

	for _, featuresAndView := range requestedFeatureViews {
		featureView := featuresAndView.View
		var joinKeyToAliasMap map[string]string
		if featureView.Base.Projection != nil && featureView.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = featureView.Base.Projection.JoinKeyMap
		} else {
			joinKeyToAliasMap = map[string]string{}
		}

		for _, entityName := range featureView.EntityNames {
			joinKey := entitiesByName[entityName].JoinKey
			entityNameToJoinKeyMap[entityName] = joinKey

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				expectedJoinKeysSet[alias] = nil
			} else {
				expectedJoinKeysSet[joinKey] = nil
			}
		}
	}
	return entityNameToJoinKeyMap, expectedJoinKeysSet, nil
}

func ValidateEntityValues(joinKeyValues map[string]*prototypes.RepeatedValue,
	requestData map[string]*prototypes.RepeatedValue,
	expectedJoinKeysSet map[string]interface{}) (int, error) {
	numRows := -1

	for joinKey, values := range joinKeyValues {
		if _, ok := expectedJoinKeysSet[joinKey]; !ok {
			requestData[joinKey] = values
			delete(joinKeyValues, joinKey)
			// ToDo: when request data will be passed correctly (not as part of entity rows)
			// ToDo: throw this error instead
			// return 0, fmt.Errorf("JoinKey is not expected in this request: %s\n%v", JoinKey, expectedJoinKeysSet)
		} else {
			if numRows < 0 {
				numRows = len(values.Val)
			} else if len(values.Val) != numRows {
				return -1, errors.New("valueError: All entity rows must have the same columns")
			}

		}
	}

	return numRows, nil
}

func ValidateFeatureRefs(requestedFeatures []*FeatureViewAndRefs, fullFeatureNames bool) error {
	featureRefCounter := make(map[string]int)
	featureRefs := make([]string, 0)
	for _, viewAndFeatures := range requestedFeatures {
		for _, feature := range viewAndFeatures.FeatureRefs {
			projectedViewName := viewAndFeatures.View.Base.Name
			if viewAndFeatures.View.Base.Projection != nil {
				projectedViewName = viewAndFeatures.View.Base.Projection.NameToUse()
			}

			featureRefs = append(featureRefs,
				fmt.Sprintf("%s:%s", projectedViewName, feature))
		}
	}

	for _, featureRef := range featureRefs {
		if fullFeatureNames {
			featureRefCounter[featureRef]++
		} else {
			_, featureName, _ := ParseFeatureReference(featureRef)
			featureRefCounter[featureName]++
		}

	}
	for featureName, occurrences := range featureRefCounter {
		if occurrences == 1 {
			delete(featureRefCounter, featureName)
		}
	}
	if len(featureRefCounter) >= 1 {
		collidedFeatureRefs := make([]string, 0)
		for collidedFeatureRef := range featureRefCounter {
			if fullFeatureNames {
				collidedFeatureRefs = append(collidedFeatureRefs, collidedFeatureRef)
			} else {
				for _, featureRef := range featureRefs {
					_, featureName, _ := ParseFeatureReference(featureRef)
					if featureName == collidedFeatureRef {
						collidedFeatureRefs = append(collidedFeatureRefs, featureRef)
					}
				}
			}
		}
		return featureNameCollisionError{collidedFeatureRefs, fullFeatureNames}
	}

	return nil
}

func TransposeFeatureRowsIntoColumns(featureData2D [][]onlinestore.FeatureData,
	groupRef *GroupedFeaturesPerEntitySet,
	requestedFeatureViews []*FeatureViewAndRefs,
	arrowAllocator memory.Allocator,
	numRows int) ([]*FeatureVector, error) {

	numFeatures := len(groupRef.AliasedFeatureNames)
	fvs := make(map[string]*model.FeatureView)
	for _, viewAndRefs := range requestedFeatureViews {
		fvs[viewAndRefs.View.Base.Name] = viewAndRefs.View
	}

	var value *prototypes.Value
	var status serving.FieldStatus
	var eventTimeStamp *timestamppb.Timestamp
	var featureData *onlinestore.FeatureData
	var fv *model.FeatureView
	var featureViewName string

	vectors := make([]*FeatureVector, 0)

	for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
		currentVector := &FeatureVector{
			Name:       groupRef.AliasedFeatureNames[featureIndex],
			Statuses:   make([]serving.FieldStatus, numRows),
			Timestamps: make([]*timestamppb.Timestamp, numRows),
		}
		vectors = append(vectors, currentVector)
		protoValues := make([]*prototypes.Value, numRows)

		for rowEntityIndex, outputIndexes := range groupRef.Indices {
			if featureData2D[rowEntityIndex] == nil {
				value = nil
				status = serving.FieldStatus_NOT_FOUND
				eventTimeStamp = &timestamppb.Timestamp{}
			} else {
				featureData = &featureData2D[rowEntityIndex][featureIndex]
				eventTimeStamp = &timestamppb.Timestamp{Seconds: featureData.Timestamp.Seconds, Nanos: featureData.Timestamp.Nanos}
				featureViewName = featureData.Reference.FeatureViewName
				fv = fvs[featureViewName]
				if _, ok := featureData.Value.Val.(*prototypes.Value_NullVal); ok {
					value = nil
					status = serving.FieldStatus_NOT_FOUND
				} else if checkOutsideTtl(eventTimeStamp, timestamppb.Now(), fv.Ttl) {
					value = &prototypes.Value{Val: featureData.Value.Val}
					status = serving.FieldStatus_OUTSIDE_MAX_AGE
				} else {
					value = &prototypes.Value{Val: featureData.Value.Val}
					status = serving.FieldStatus_PRESENT
				}
			}
			for _, rowIndex := range outputIndexes {
				protoValues[rowIndex] = value
				currentVector.Statuses[rowIndex] = status
				currentVector.Timestamps[rowIndex] = eventTimeStamp
			}
		}
		arrowValues, err := types.ProtoValuesToArrowArray(protoValues, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}
		currentVector.Values = arrowValues
	}

	return vectors, nil

}

func KeepOnlyRequestedFeatures(
	vectors []*FeatureVector,
	requestedFeatureRefs []string,
	featureService *model.FeatureService,
	fullFeatureNames bool) ([]*FeatureVector, error) {
	vectorsByName := make(map[string]*FeatureVector)
	expectedVectors := make([]*FeatureVector, 0)

	usedVectors := make(map[string]bool)

	for _, vector := range vectors {
		vectorsByName[vector.Name] = vector
	}

	if featureService != nil {
		for _, projection := range featureService.Projections {
			for _, f := range projection.Features {
				requestedFeatureRefs = append(requestedFeatureRefs,
					fmt.Sprintf("%s:%s", projection.NameToUse(), f.Name))
			}
		}
	}

	for _, featureRef := range requestedFeatureRefs {
		viewName, featureName, err := ParseFeatureReference(featureRef)
		if err != nil {
			return nil, err
		}
		qualifiedName := getQualifiedFeatureName(viewName, featureName, fullFeatureNames)
		if _, ok := vectorsByName[qualifiedName]; !ok {
			return nil, fmt.Errorf("requested feature %s can't be retrieved", featureRef)
		}
		expectedVectors = append(expectedVectors, vectorsByName[qualifiedName])
		usedVectors[qualifiedName] = true
	}

	// Free arrow arrays for vectors that were not used.
	for _, vector := range vectors {
		if _, ok := usedVectors[vector.Name]; !ok {
			vector.Values.Release()
		}
	}

	return expectedVectors, nil
}

func EntitiesToFeatureVectors(entityColumns map[string]*prototypes.RepeatedValue, arrowAllocator memory.Allocator, numRows int) ([]*FeatureVector, error) {
	vectors := make([]*FeatureVector, 0)
	presentVector := make([]serving.FieldStatus, numRows)
	timestampVector := make([]*timestamppb.Timestamp, numRows)
	for idx := 0; idx < numRows; idx++ {
		presentVector[idx] = serving.FieldStatus_PRESENT
		timestampVector[idx] = timestamppb.Now()
	}
	for entityName, values := range entityColumns {
		arrowColumn, err := types.ProtoValuesToArrowArray(values.Val, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, &FeatureVector{
			Name:       entityName,
			Values:     arrowColumn,
			Statuses:   presentVector,
			Timestamps: timestampVector,
		})
	}
	return vectors, nil
}

func ParseFeatureReference(featureRef string) (featureViewName, featureName string, e error) {
	parsedFeatureName := strings.Split(featureRef, ":")

	if len(parsedFeatureName) == 0 {
		e = errors.New("featureReference should be in the format: 'FeatureViewName:FeatureName'")
	} else if len(parsedFeatureName) == 1 {
		featureName = parsedFeatureName[0]
	} else {
		featureViewName = parsedFeatureName[0]
		featureName = parsedFeatureName[1]
	}
	return
}

func entityKeysToProtos(joinKeyValues map[string]*prototypes.RepeatedValue) []*prototypes.EntityKey {
	keys := make([]string, len(joinKeyValues))
	index := 0
	var numRows int
	for k, v := range joinKeyValues {
		keys[index] = k
		index += 1
		numRows = len(v.Val)
	}
	sort.Strings(keys)
	entityKeys := make([]*prototypes.EntityKey, numRows)
	numJoinKeys := len(keys)
	// Construct each EntityKey object
	for index = 0; index < numRows; index++ {
		entityKeys[index] = &prototypes.EntityKey{JoinKeys: keys, EntityValues: make([]*prototypes.Value, numJoinKeys)}
	}

	for colIndex, key := range keys {
		for index, value := range joinKeyValues[key].GetVal() {
			entityKeys[index].EntityValues[colIndex] = value
		}
	}
	return entityKeys
}

func GroupFeatureRefs(requestedFeatureViews []*FeatureViewAndRefs,
	joinKeyValues map[string]*prototypes.RepeatedValue,
	entityNameToJoinKeyMap map[string]string,
	fullFeatureNames bool,
) (map[string]*GroupedFeaturesPerEntitySet,
	error,
) {
	groups := make(map[string]*GroupedFeaturesPerEntitySet)

	for _, featuresAndView := range requestedFeatureViews {
		joinKeys := make([]string, 0)
		fv := featuresAndView.View
		featureNames := featuresAndView.FeatureRefs
		for _, entityName := range fv.EntityNames {
			joinKeys = append(joinKeys, entityNameToJoinKeyMap[entityName])
		}

		groupKeyBuilder := make([]string, 0)
		joinKeysValuesProjection := make(map[string]*prototypes.RepeatedValue)

		joinKeyToAliasMap := make(map[string]string)
		if fv.Base.Projection != nil && fv.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = fv.Base.Projection.JoinKeyMap
		}

		for _, joinKey := range joinKeys {
			var joinKeyOrAlias string

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				groupKeyBuilder = append(groupKeyBuilder, fmt.Sprintf("%s[%s]", joinKey, alias))
				joinKeyOrAlias = alias
			} else {
				groupKeyBuilder = append(groupKeyBuilder, joinKey)
				joinKeyOrAlias = joinKey
			}

			if _, ok := joinKeyValues[joinKeyOrAlias]; !ok {
				return nil, fmt.Errorf("key %s is missing in provided entity rows", joinKey)
			}
			joinKeysValuesProjection[joinKey] = joinKeyValues[joinKeyOrAlias]
		}

		sort.Strings(groupKeyBuilder)
		groupKey := strings.Join(groupKeyBuilder, ",")

		aliasedFeatureNames := make([]string, 0)
		featureViewNames := make([]string, 0)
		var viewNameToUse string
		if fv.Base.Projection != nil {
			viewNameToUse = fv.Base.Projection.NameToUse()
		} else {
			viewNameToUse = fv.Base.Name
		}

		for _, featureName := range featureNames {
			aliasedFeatureNames = append(aliasedFeatureNames,
				getQualifiedFeatureName(viewNameToUse, featureName, fullFeatureNames))
			featureViewNames = append(featureViewNames, fv.Base.Name)
		}

		if _, ok := groups[groupKey]; !ok {
			joinKeysProto := entityKeysToProtos(joinKeysValuesProjection)
			uniqueEntityRows, mappingIndices, err := getUniqueEntityRows(joinKeysProto)
			if err != nil {
				return nil, err
			}

			groups[groupKey] = &GroupedFeaturesPerEntitySet{
				FeatureNames:        featureNames,
				FeatureViewNames:    featureViewNames,
				AliasedFeatureNames: aliasedFeatureNames,
				Indices:             mappingIndices,
				EntityKeys:          uniqueEntityRows,
			}

		} else {
			groups[groupKey].FeatureNames = append(groups[groupKey].FeatureNames, featureNames...)
			groups[groupKey].AliasedFeatureNames = append(groups[groupKey].AliasedFeatureNames, aliasedFeatureNames...)
			groups[groupKey].FeatureViewNames = append(groups[groupKey].FeatureViewNames, featureViewNames...)
		}
	}
	return groups, nil
}

func getUniqueEntityRows(joinKeysProto []*prototypes.EntityKey) ([]*prototypes.EntityKey, [][]int, error) {
	uniqueValues := make(map[[sha256.Size]byte]*prototypes.EntityKey, 0)
	positions := make(map[[sha256.Size]byte][]int, 0)

	for index, entityKey := range joinKeysProto {
		serializedRow, err := proto.Marshal(entityKey)
		if err != nil {
			return nil, nil, err
		}

		rowHash := sha256.Sum256(serializedRow)
		if _, ok := uniqueValues[rowHash]; !ok {
			uniqueValues[rowHash] = entityKey
			positions[rowHash] = []int{index}
		} else {
			positions[rowHash] = append(positions[rowHash], index)
		}
	}

	mappingIndices := make([][]int, len(uniqueValues))
	uniqueEntityRows := make([]*prototypes.EntityKey, 0)
	for rowHash, row := range uniqueValues {
		nextIdx := len(uniqueEntityRows)

		mappingIndices[nextIdx] = positions[rowHash]
		uniqueEntityRows = append(uniqueEntityRows, row)
	}
	return uniqueEntityRows, mappingIndices, nil
}

func checkOutsideTtl(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	if ttl.Seconds == 0 {
		return false
	}
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
}

func getQualifiedFeatureName(viewName string, featureName string, fullFeatureNames bool) string {
	if fullFeatureNames {
		return fmt.Sprintf("%s__%s", viewName, featureName)
	} else {
		return featureName
	}
}

type featureNameCollisionError struct {
	featureRefCollisions []string
	fullFeatureNames     bool
}

func (e featureNameCollisionError) Error() string {
	return fmt.Sprintf("featureNameCollisionError: %s; %t", strings.Join(e.featureRefCollisions, ", "), e.fullFeatureNames)
}
