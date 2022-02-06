package feast

import (
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"strings"
)

type redisType int

const (
	redisNode    redisType = 0
	redisCluster redisType = 1
)

type RedisOnlineStore struct {
	// Redis database type, either a single node server (RedisType.Redis) or a cluster (RedisType.RedisCluster)
	t redisType
	// List of Redis hostnames in format `host:port`
	addrs []string
	// Redis password
	password string
	// Redis connection encryption
	ssl bool
}

func NewRedisOnlineStore(onlineStoreConfig map[string]interface{}) (*RedisOnlineStore, error) {
	r := RedisOnlineStore{}
	// Parse redis_type and write it into r.t
	redisTypeJson, ok := onlineStoreConfig["redis_type"]
	if !ok {
		// default to "redis"
		redisTypeJson = "redis"
	} else if redisTypeStr, ok := redisTypeJson.(string); !ok {
		return nil, errors.New(fmt.Sprintf("Failed to convert redis_type to string: %+v", redisTypeJson))
	} else {
		if redisTypeStr == "redis" {
			r.t = redisNode
		} else if redisTypeStr == "redis_cluster" {
			r.t = redisCluster
		} else {
			return nil, errors.New(fmt.Sprintf("Failed to convert redis_type to enum: %s. Must be one of 'redis', 'redis_cluster'", redisTypeStr))
		}
	}
	// Parse connection_string and write it into r.addrs, r.password, and r.ssl
	redisConnJson, ok := onlineStoreConfig["connection_string"]
	if !ok {
		// default to "localhost:6379"
		redisConnJson = "localhost:6379"
	}
	if redisConnStr, ok := redisConnJson.(string); !ok {
		return nil, errors.New(fmt.Sprintf("Failed to convert connection_string to string: %+v", redisConnJson))
	} else {
		parts := strings.Split(redisConnStr, ",")
		for _, part := range parts {
			if strings.Contains(part, ":") {
				r.addrs = append(r.addrs, part)
			} else if strings.Contains(part, "=") {
				kv := strings.SplitN(part, "=", 2)
				if kv[0] == "password" {
					r.password = kv[1]
				} else if kv[0] == "ssl" {
					r.ssl = kv[1] == "true"
				} else {
					return nil, errors.New(fmt.Sprintf("Unrecognized option in connection_string: %s. Must be one of 'password', 'ssl'", kv[0]))
				}
			} else {
				return nil, errors.New(fmt.Sprintf("Unable to parse a part of connection_string: %s. Must contain either ':' (addresses) or '=' (options", part))
			}
		}
	}
	return &r, nil
}

func (r *RedisOnlineStore) OnlineRead(entityKeys []types.EntityKey, featureReferences []serving.FeatureReferenceV2) ([][]Feature, error) {
	return nil, errors.New("not implemented")
}
