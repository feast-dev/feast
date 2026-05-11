package onlinestore

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/types"

	"github.com/stretchr/testify/assert"
)

func TestNewValkeyOnlineStore(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "localhost:6379",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewValkeyOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.Equal(t, "localhost:6379", store.opts.InitAddress[0])
	assert.Equal(t, "", store.opts.Password)
	assert.Equal(t, 0, store.opts.SelectDB)
	assert.Nil(t, store.opts.TLSConfig)
}

func TestNewValkeyOnlineStoreWithPassword(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "localhost:6379,password=secret",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewValkeyOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.Equal(t, "localhost:6379", store.opts.InitAddress[0])
	assert.Equal(t, "secret", store.opts.Password)
}

func TestNewValkeyOnlineStoreWithDB(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "localhost:6379,db=1",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewValkeyOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.Equal(t, "localhost:6379", store.opts.InitAddress[0])
	assert.Equal(t, 1, store.opts.SelectDB)
}

func TestNewValkeyOnlineStoreWithSsl(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "localhost:6379,ssl=true",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewValkeyOnlineStore("test", rc, config)
	assert.Nil(t, err)
	assert.Equal(t, "localhost:6379", store.opts.InitAddress[0])
	assert.NotNil(t, store.opts.TLSConfig)
}

func TestValkeyBuildFeatureViewIndices(t *testing.T) {
	r := &ValkeyOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 0, index)
	})

	t.Run("test with non-empty featureNames and empty featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{"feature1", "feature2"})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 2, index)
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view2"}, []string{"feature1", "feature2"})
		assert.Equal(t, 2, len(featureViewIndices))
		assert.Equal(t, 2, len(indicesFeatureView))
		assert.Equal(t, 4, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
		assert.Equal(t, "view2", indicesFeatureView[3])
	})

	t.Run("test with duplicate featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view1"}, []string{"feature1", "feature2"})
		assert.Equal(t, 1, len(featureViewIndices))
		assert.Equal(t, 1, len(indicesFeatureView))
		assert.Equal(t, 3, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
	})
}

func TestValkeyBuildValkeyKeys(t *testing.T) {
	r := &ValkeyOnlineStore{
		project: "test_project",
		config: &registry.RepoConfig{
			EntityKeySerializationVersion: 3,
		},
	}

	entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	entity_key2 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}},
	}

	error_entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id", "vehicle_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	t.Run("test with empty entityKeys", func(t *testing.T) {
		valkeyKeys, valkeyKeyToEntityIndex, err := r.buildValkeyKeys([]*types.EntityKey{})
		assert.Nil(t, err)
		assert.Equal(t, 0, len(valkeyKeys))
		assert.Equal(t, 0, len(valkeyKeyToEntityIndex))
	})

	t.Run("test with single entityKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&entity_key1}
		valkeyKeys, valkeyKeyToEntityIndex, err := r.buildValkeyKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(valkeyKeys))
		assert.Equal(t, 1, len(valkeyKeyToEntityIndex))
	})

	t.Run("test with multiple entityKeys", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&entity_key1, &entity_key2}
		valkeyKeys, valkeyKeyToEntityIndex, err := r.buildValkeyKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(valkeyKeys))
		assert.Equal(t, 2, len(valkeyKeyToEntityIndex))
	})

	t.Run("test with error in buildValkeyKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&error_entity_key1}
		_, _, err := r.buildValkeyKeys(entityKeys)
		assert.NotNil(t, err)
	})
}
