package onlinestore

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/stretchr/testify/assert"
)

func TestNewDynamodbOnlineStore(t *testing.T) {
	var config = map[string]interface{}{
		"batch_size":           40,
		"region":               "us-east-1",
		"max_pool_connections": 4,
		"consistent_reads":     "true",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 2,
	}
	_, err := NewDynamodbOnlineStore("test", rc, config)
	assert.Nil(t, err)
}
