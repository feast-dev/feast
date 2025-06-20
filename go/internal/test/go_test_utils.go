package test

import (
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func CreateFeatureService(serviceName string, viewProjections map[string][]*core.FeatureSpecV2) *model.FeatureService {
	fsProto := CreateFeatureServiceProto(serviceName, viewProjections)
	return model.NewFeatureServiceFromProto(fsProto)
}

func CreateOnDemandFeatureViewProto(name string, featureSources map[string][]*core.FeatureSpecV2, features ...*core.FeatureSpecV2) *core.OnDemandFeatureView {
	sources := make(map[string]*core.OnDemandSource)
	for viewName, featureColumn := range featureSources {
		sources[viewName] = &core.OnDemandSource{
			Source: &core.OnDemandSource_FeatureViewProjection{
				FeatureViewProjection: &core.FeatureViewProjection{
					FeatureViewName: viewName,
					FeatureColumns:  featureColumn,
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
	return proto
}

func CreateBaseFeatureView(name string, features []*model.Field, projection *model.FeatureViewProjection) *model.BaseFeatureView {
	return &model.BaseFeatureView{
		Name:       name,
		Features:   features,
		Projection: projection,
	}
}

func CreateNewEntity(name string, joinKey string) *model.Entity {
	return &model.Entity{
		Name:    name,
		JoinKey: joinKey,
	}
}

func CreateNewField(name string, dtype types.ValueType_Enum) *model.Field {
	return &model.Field{Name: name,
		Dtype: dtype,
	}
}

func CreateNewFeatureService(name string, project string, createdTimestamp *timestamppb.Timestamp, lastUpdatedTimestamp *timestamppb.Timestamp, projections []*model.FeatureViewProjection) *model.FeatureService {
	return &model.FeatureService{
		Name:                 name,
		Project:              project,
		CreatedTimestamp:     createdTimestamp,
		LastUpdatedTimestamp: lastUpdatedTimestamp,
		Projections:          projections,
	}
}

func CreateNewFeatureViewProjection(name string, nameAlias string, features []*model.Field, joinKeyMap map[string]string) *model.FeatureViewProjection {
	return &model.FeatureViewProjection{Name: name,
		NameAlias:  nameAlias,
		Features:   features,
		JoinKeyMap: joinKeyMap,
	}
}

func CreateFeatureView(base *model.BaseFeatureView, ttl *durationpb.Duration, entities []string, entityColumns []*model.Field) *model.FeatureView {
	return &model.FeatureView{
		Base:          base,
		Ttl:           ttl,
		EntityNames:   entities,
		EntityColumns: entityColumns,
	}
}

func CreateSortedFeatureView(base *model.BaseFeatureView, ttl *durationpb.Duration, entities []string, entityColumns []*model.Field, sortKeys []*model.SortKey) *model.SortedFeatureView {
	return &model.SortedFeatureView{
		FeatureView: CreateFeatureView(base, ttl, entities, entityColumns),
		SortKeys:    sortKeys,
	}
}

func CreateEntityProto(name string, valueType types.ValueType_Enum, joinKey string) *core.Entity {
	return &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:      name,
			ValueType: valueType,
			JoinKey:   joinKey,
		},
	}
}

func CreateFeature(name string, valueType types.ValueType_Enum) *core.FeatureSpecV2 {
	return &core.FeatureSpecV2{
		Name:      name,
		ValueType: valueType,
	}
}

func CreateFeatureViewProto(name string, entities []*core.Entity, features ...*core.FeatureSpecV2) *core.FeatureView {
	entityNames, entityColumns := getEntityNamesAndColumns(entities)
	viewProto := core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name:          name,
			Entities:      entityNames,
			Features:      features,
			Ttl:           &durationpb.Duration{},
			EntityColumns: entityColumns,
		},
	}
	return &viewProto
}

func CreateSortKeyProto(name string, order core.SortOrder_Enum, valueType types.ValueType_Enum) *core.SortKey {
	return &core.SortKey{
		Name:             name,
		DefaultSortOrder: order,
		ValueType:        valueType,
	}
}

func CreateSortedFeatureViewProto(name string, entities []*core.Entity, sortKeys []*core.SortKey, features ...*core.FeatureSpecV2) *core.SortedFeatureView {
	entityNames, entityColumns := getEntityNamesAndColumns(entities)
	viewProto := core.SortedFeatureView{
		Spec: &core.SortedFeatureViewSpec{
			Name:          name,
			Entities:      entityNames,
			Features:      features,
			SortKeys:      sortKeys,
			Ttl:           &durationpb.Duration{},
			EntityColumns: entityColumns,
		},
	}
	return &viewProto
}

func CreateSortedFeatureViewModel(name string, entities []*core.Entity, sortKeys []*core.SortKey, features ...*core.FeatureSpecV2) *model.SortedFeatureView {
	viewProto := CreateSortedFeatureViewProto(name, entities, sortKeys, features...)
	return model.NewSortedFeatureViewFromProto(viewProto)
}

func CreateFeatureServiceProto(name string, viewProjections map[string][]*core.FeatureSpecV2) *core.FeatureService {
	projections := make([]*core.FeatureViewProjection, 0)
	for name, features := range viewProjections {
		projections = append(projections, &core.FeatureViewProjection{
			FeatureViewName: name,
			FeatureColumns:  features,
			JoinKeyMap:      map[string]string{},
		})
	}

	return &core.FeatureService{
		Spec: &core.FeatureServiceSpec{
			Name:     name,
			Features: projections,
		},
		Meta: &core.FeatureServiceMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}
}

func getEntityNamesAndColumns(entities []*core.Entity) ([]string, []*core.FeatureSpecV2) {
	entityNames := make([]string, len(entities))
	entityColumns := make([]*core.FeatureSpecV2, len(entities))
	for i, entity := range entities {
		entityNames[i] = entity.Spec.Name
		entityColumns[i] = &core.FeatureSpecV2{
			Name:      entity.Spec.JoinKey,
			ValueType: entity.Spec.ValueType,
		}
	}
	return entityNames, entityColumns
}
