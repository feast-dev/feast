package onlinestore

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/types"

	"github.com/stretchr/testify/assert"
)

func TestNewRedisOnlineStore(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.Password, "")
	assert.Equal(t, opts.DB, 0)
	assert.Nil(t, opts.TLSConfig)
}

func TestNewRedisOnlineStoreWithPassword(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,password=secret",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.Password, "secret")
}

func TestNewRedisOnlineStoreWithDB(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,db=1",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.DB, 1)
}

func TestNewRedisOnlineStoreWithSsl(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,ssl=true",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.NotNil(t, opts.TLSConfig)
}

func TestBuildFeatureViewIndices(t *testing.T) {
	r := &RedisOnlineStore{}

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

func TestBuildHsetKeys(t *testing.T) {
	r := &RedisOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{}, []string{}, map[int]string{}, 0)
		assert.Equal(t, 0, len(hsetKeys))
		assert.Equal(t, 0, len(featureNames))
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{"view1", "view2"}, []string{"feature1", "feature2"}, map[int]string{2: "view1", 3: "view2"}, 4)
		assert.Equal(t, 4, len(hsetKeys))
		assert.Equal(t, 4, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[2])
		assert.Equal(t, "_ts:view2", hsetKeys[3])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
	})

	t.Run("test with more featureViewNames than featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{"view1", "view2", "view3"}, []string{"feature1", "feature2", "feature3"}, map[int]string{3: "view1", 4: "view2", 5: "view3"}, 6)
		assert.Equal(t, 6, len(hsetKeys))
		assert.Equal(t, 6, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[3])
		assert.Equal(t, "_ts:view2", hsetKeys[4])
		assert.Equal(t, "_ts:view3", hsetKeys[5])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
		assert.Contains(t, featureNames, "_ts:view3")
	})
}

func TestBuildRedisKeys(t *testing.T) {
	r := &RedisOnlineStore{
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
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys([]*types.EntityKey{})
		assert.Nil(t, err)
		assert.Equal(t, 0, len(redisKeys))
		assert.Equal(t, 0, len(redisKeyToEntityIndex))
	})

	t.Run("test with single entityKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&entity_key1}
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(redisKeys))
		assert.Equal(t, 1, len(redisKeyToEntityIndex))
	})

	t.Run("test with multiple entityKeys", func(t *testing.T) {
		entityKeys := []*types.EntityKey{
			&entity_key1, &entity_key2,
		}
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(redisKeys))
		assert.Equal(t, 2, len(redisKeyToEntityIndex))
	})

	t.Run("test with error in buildRedisKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&error_entity_key1}
		_, _, err := r.buildRedisKeys(entityKeys)
		assert.NotNil(t, err)
	})
}
