package onlinestore

import (
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRedisOnlineStore(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 2,
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
		EntityKeySerializationVersion: 2,
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
		EntityKeySerializationVersion: 2,
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
		EntityKeySerializationVersion: 2,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.NotNil(t, opts.TLSConfig)
}
