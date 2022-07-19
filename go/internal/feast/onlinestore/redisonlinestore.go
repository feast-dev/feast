package onlinestore

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"sort"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
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

	config *registry.RepoConfig
}

func NewRedisOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*RedisOnlineStore, error) {
	store := RedisOnlineStore{
		project: project,
		config:  config,
	}

	var address []string
	var password string
	var tlsConfig *tls.Config
	var db int // Default to 0

	// Parse redis_type and write it into conf.t
	t, err := getRedisType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}

	// Parse connection_string and write it into conf.address, conf.password, and conf.ssl
	redisConnJson, ok := onlineStoreConfig["connection_string"]
	if !ok {
		// Default to "localhost:6379"
		redisConnJson = "localhost:6379"
	}
	if redisConnStr, ok := redisConnJson.(string); !ok {
		return nil, errors.New(fmt.Sprintf("failed to convert connection_string to string: %+v", redisConnJson))
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
					result, err := strconv.ParseBool(kv[1])
					if err != nil {
						return nil, err
					} else if result {
						tlsConfig = &tls.Config{}
					}
				} else if kv[0] == "db" {
					db, err = strconv.Atoi(kv[1])
					if err != nil {
						return nil, err
					}
				} else {
					return nil, errors.New(fmt.Sprintf("unrecognized option in connection_string: %s. Must be one of 'password', 'ssl'", kv[0]))
				}
			} else {
				return nil, errors.New(fmt.Sprintf("unable to parse a part of connection_string: %s. Must contain either ':' (addresses) or '=' (options", part))
			}
		}
	}

	if t == redisNode {
		store.client = redis.NewClient(&redis.Options{
			Addr:      address[0],
			Password:  password, // No password set
			DB:        db,
			TLSConfig: tlsConfig,
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
		// Default to "redis"
		redisTypeJson = "redis"
	} else if redisTypeStr, ok := redisTypeJson.(string); !ok {
		return -1, errors.New(fmt.Sprintf("failed to convert redis_type to string: %+v", redisTypeJson))
	} else {
		if redisTypeStr == "redis" {
			t = redisNode
		} else if redisTypeStr == "redis_cluster" {
			t = redisCluster
		} else {
			return -1, errors.New(fmt.Sprintf("failed to convert redis_type to enum: %s. Must be one of 'redis', 'redis_cluster'", redisTypeStr))
		}
	}
	return t, nil
}

func (r *RedisOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	featureCount := len(featureNames)
	index := featureCount
	featureViewIndices := make(map[string]int)
	indicesFeatureView := make(map[int]string)
	for _, featureViewName := range featureViewNames {
		if _, ok := featureViewIndices[featureViewName]; !ok {
			featureViewIndices[featureViewName] = index
			indicesFeatureView[index] = featureViewName
			index += 1
		}
	}
	var hsetKeys = make([]string, index)
	h := murmur3.New32()
	intBuffer := h.Sum32()
	byteBuffer := make([]byte, 4)

	for i := 0; i < featureCount; i++ {
		h.Write([]byte(fmt.Sprintf("%s:%s", featureViewNames[i], featureNames[i])))
		intBuffer = h.Sum32()
		binary.LittleEndian.PutUint32(byteBuffer, intBuffer)
		hsetKeys[i] = string(byteBuffer)
		h.Reset()
	}
	for i := featureCount; i < index; i++ {
		view := indicesFeatureView[i]
		tsKey := fmt.Sprintf("_ts:%s", view)
		hsetKeys[i] = tsKey
		featureNames = append(featureNames, tsKey)
	}

	redisKeys := make([]*[]byte, len(entityKeys))
	redisKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {

		var key, err = buildRedisKey(r.project, entityKeys[i], r.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		redisKeys[i] = key
		redisKeyToEntityIndex[string(*key)] = i
	}

	// Retrieve features from Redis
	// TODO: Move context object out

	results := make([][]FeatureData, len(entityKeys))
	pipe := r.client.Pipeline()
	commands := map[string]*redis.SliceCmd{}

	for _, redisKey := range redisKeys {
		keyString := string(*redisKey)
		commands[keyString] = pipe.HMGet(ctx, keyString, hsetKeys...)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	var entityIndex int
	var resContainsNonNil bool
	for redisKey, values := range commands {

		entityIndex = redisKeyToEntityIndex[redisKey]
		resContainsNonNil = false

		results[entityIndex] = make([]FeatureData, featureCount)
		res, err := values.Result()
		if err != nil {
			return nil, err
		}

		var timeStamp timestamppb.Timestamp

		for featureIndex, resString := range res {
			if featureIndex == featureCount {
				break
			}

			if resString == nil {
				// TODO (Ly): Can there be nil result within each feature or they will all be returned as string proto of types.Value_NullVal proto?
				featureName := featureNames[featureIndex]
				featureViewName := featureViewNames[featureIndex]
				timeStampIndex := featureViewIndices[featureViewName]
				timeStampInterface := res[timeStampIndex]
				if timeStampInterface != nil {
					if timeStampString, ok := timeStampInterface.(string); !ok {
						return nil, errors.New("error parsing value from redis")
					} else {
						if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
							return nil, errors.New("error converting parsed redis value to timestamppb.Timestamp")
						}
					}
				}

				results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
					Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
					Value:     types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
				}

			} else if valueString, ok := resString.(string); !ok {
				return nil, errors.New("error parsing Value from redis")
			} else {
				resContainsNonNil = true
				var value types.Value
				if err := proto.Unmarshal([]byte(valueString), &value); err != nil {
					return nil, errors.New("error converting parsed redis Value to types.Value")
				} else {
					featureName := featureNames[featureIndex]
					featureViewName := featureViewNames[featureIndex]
					timeStampIndex := featureViewIndices[featureViewName]
					timeStampInterface := res[timeStampIndex]
					if timeStampInterface != nil {
						if timeStampString, ok := timeStampInterface.(string); !ok {
							return nil, errors.New("error parsing Value from redis")
						} else {
							if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
								return nil, errors.New("error converting parsed redis Value to timestamppb.Timestamp")
							}
						}
					}
					results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
						Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
						Value:     types.Value{Val: value.Val},
					}
				}
			}
		}

		if !resContainsNonNil {
			results[entityIndex] = nil
		}

	}

	return results, nil
}

// Dummy destruct function to conform with plugin OnlineStore interface
func (r *RedisOnlineStore) Destruct() {

}

func buildRedisKey(project string, entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	serKey, err := serializeEntityKey(entityKey, entityKeySerializationVersion)
	if err != nil {
		return nil, err
	}
	fullKey := append(*serKey, []byte(project)...)
	return &fullKey, nil
}

func serializeEntityKey(entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	// Serialize entity key to a bytestring so that it can be used as a lookup key in a hash table.

	// Ensure that we have the right amount of join keys and entity values
	if len(entityKey.JoinKeys) != len(entityKey.EntityValues) {
		return nil, errors.New(fmt.Sprintf("the amount of join key names and entity values don't match: %s vs %s", entityKey.JoinKeys, entityKey.EntityValues))
	}

	// Make sure that join keys are sorted so that we have consistent key building
	m := make(map[string]*types.Value)

	for i := 0; i < len(entityKey.JoinKeys); i++ {
		m[entityKey.JoinKeys[i]] = entityKey.EntityValues[i]
	}

	keys := make([]string, 0, len(m))
	for k := range entityKey.JoinKeys {
		keys = append(keys, entityKey.JoinKeys[k])
	}
	sort.Strings(keys)

	// Build the key
	length := 5 * len(keys)
	bufferList := make([][]byte, length)

	for i := 0; i < len(keys); i++ {
		offset := i * 2
		byteBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(byteBuffer, uint32(types.ValueType_Enum_value["STRING"]))
		bufferList[offset] = byteBuffer
		bufferList[offset+1] = []byte(keys[i])
	}

	for i := 0; i < len(keys); i++ {
		offset := (2 * len(keys)) + (i * 3)
		value := m[keys[i]].GetVal()

		valueBytes, valueTypeBytes, err := serializeValue(value, entityKeySerializationVersion)
		if err != nil {
			return valueBytes, err
		}

		typeBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(typeBuffer, uint32(valueTypeBytes))

		lenBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuffer, uint32(len(*valueBytes)))

		bufferList[offset+0] = typeBuffer
		bufferList[offset+1] = lenBuffer
		bufferList[offset+2] = *valueBytes
	}

	// Convert from an array of byte arrays to a single byte array
	var entityKeyBuffer []byte
	for i := 0; i < len(bufferList); i++ {
		entityKeyBuffer = append(entityKeyBuffer, bufferList[i]...)
	}

	return &entityKeyBuffer, nil
}

func serializeValue(value interface{}, entityKeySerializationVersion int64) (*[]byte, types.ValueType_Enum, error) {
	// TODO: Implement support for other types (at least the major types like ints, strings, bytes)
	switch x := (value).(type) {
	case *types.Value_StringVal:
		valueString := []byte(x.StringVal)
		return &valueString, types.ValueType_STRING, nil
	case *types.Value_BytesVal:
		return &x.BytesVal, types.ValueType_BYTES, nil
	case *types.Value_Int32Val:
		valueBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueBuffer, uint32(x.Int32Val))
		return &valueBuffer, types.ValueType_INT32, nil
	case *types.Value_Int64Val:
		if entityKeySerializationVersion <= 1 {
			//  We unfortunately have to use 32 bit here for backward compatibility :(
			valueBuffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(valueBuffer, uint32(x.Int64Val))
			return &valueBuffer, types.ValueType_INT64, nil
		} else {
			valueBuffer := make([]byte, 8)
			binary.LittleEndian.PutUint64(valueBuffer, uint64(x.Int64Val))
			return &valueBuffer, types.ValueType_INT64, nil
		}
	case nil:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	default:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	}
}
