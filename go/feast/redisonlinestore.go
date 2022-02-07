package feast

import (
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/go-redis/redis/v8"
	"strings"
)

type redisType int

const (
	redisNode    redisType = 0
	redisCluster redisType = 1
)

type RedisOnlineStore struct {

	// Feast project name
	// TODO (woop): Should we remove project as state that is tracked at the store level?
	project string

	// Redis database type, either a single node server (RedisType.Redis) or a cluster (RedisType.RedisCluster)
	t redisType

	// Redis client connector
	client *redis.Client
}

func NewRedisOnlineStore(project string, onlineStoreConfig map[string]interface{}) (*RedisOnlineStore, error) {
	store := RedisOnlineStore{project: project}

	var address []string
	var password string

	// Parse redis_type and write it into conf.t
	t, err := getRedisType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}

	// Parse connection_string and write it into conf.address, conf.password, and conf.ssl
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
				address = append(address, part)
			} else if strings.Contains(part, "=") {
				kv := strings.SplitN(part, "=", 2)
				if kv[0] == "password" {
					password = kv[1]
				} else if kv[0] == "ssl" {
					// TODO (woop): Add support for TLS/SSL
					//ssl = kv[1] == "true"
				} else {
					return nil, errors.New(fmt.Sprintf("Unrecognized option in connection_string: %s. Must be one of 'password', 'ssl'", kv[0]))
				}
			} else {
				return nil, errors.New(fmt.Sprintf("Unable to parse a part of connection_string: %s. Must contain either ':' (addresses) or '=' (options", part))
			}
		}
	}

	if t == redisNode {
		store.client = redis.NewClient(&redis.Options{
			Addr:     address[0],
			Password: password, // no password set
			DB:       0,        // use default DB

		})
	} else {
		return nil, errors.New("only single node Redis is supported at this time")
	}

	return &store, nil
}

func getRedisType(onlineStoreConfig map[string]interface{}) (redisType, error) {
	var t redisType

	redisTypeJson, ok := onlineStoreConfig["redis_type"]
	if !ok {
		// default to "redis"
		redisTypeJson = "redis"
	} else if redisTypeStr, ok := redisTypeJson.(string); !ok {
		return -1, errors.New(fmt.Sprintf("Failed to convert redis_type to string: %+v", redisTypeJson))
	} else {
		if redisTypeStr == "redis" {
			t = redisNode
		} else if redisTypeStr == "redis_cluster" {
			t = redisCluster
		} else {
			return -1, errors.New(fmt.Sprintf("Failed to convert redis_type to enum: %s. Must be one of 'redis', 'redis_cluster'", redisTypeStr))
		}
	}
	return t, nil
}

func (r *RedisOnlineStore) OnlineRead(entityKeys []types.EntityKey, view string, features []string) ([][]Feature, error) {
	return nil, errors.New("not implemented")
}
