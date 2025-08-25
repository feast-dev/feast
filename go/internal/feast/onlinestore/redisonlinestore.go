package onlinestore

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"os"
	"strconv"
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/utils"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	redisprometheus "github.com/redis/go-redis/extra/redisprometheus/v9"
	redis "github.com/redis/go-redis/v9"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	redistrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/redis/go-redis.v9"
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

	// Redis cluster client connector
	clusterClient *redis.ClusterClient

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

	// Parse redis_type and write it into conf.redisStoreType
	redisStoreType, err := getRedisType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}
	store.t = redisStoreType

	// Parse connection_string and write it into conf.address, conf.password, and conf.ssl
	redisConnJson, ok := onlineStoreConfig["connection_string"]
	if !ok {
		// Default to "localhost:6379"
		redisConnJson = "localhost:6379"
	}
	if redisConnStr, ok := redisConnJson.(string); !ok {
		return nil, fmt.Errorf("failed to convert connection_string to string: %+v", redisConnJson)
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
					return nil, fmt.Errorf("unrecognized option in connection_string: %s. Must be one of 'password', 'ssl'", kv[0])
				}
			} else {
				return nil, fmt.Errorf("unable to parse a part of connection_string: %s. Must contain either ':' (addresses) or '=' (options", part)
			}
		}
	}

	// Metrics are not showing up when the service name is set to DD_SERVICE
	redisTraceServiceName := os.Getenv("DD_SERVICE") + "-redis"
	if redisTraceServiceName == "" {
		redisTraceServiceName = "redis.client" // default service name if DD_SERVICE is not set
	}

	if redisStoreType == redisNode {
		log.Info().Msgf("Using Redis: %s", address[0])
		store.client = redis.NewClient(&redis.Options{
			Addr:      address[0],
			Password:  password,
			DB:        db,
			TLSConfig: tlsConfig,
		})
		if (strings.ToLower(os.Getenv("ENABLE_DATADOG_REDIS_TRACING")) == "true") || (strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true") {
			if strings.ToLower(os.Getenv("ENABLE_DATADOG_REDIS_TRACING")) == "true" {
				log.Warn().Msg("ENABLE_DATADOG_REDIS_TRACING is deprecated. Use ENABLE_ONLINE_STORE_TRACING instead.")
			}

			redistrace.WrapClient(store.client, redistrace.WithServiceName(redisTraceServiceName))
			collector := redisprometheus.NewCollector("mlpfs", "redis", store.client)
			prometheus.MustRegister(collector)
		}
	} else if redisStoreType == redisCluster {
		log.Info().Msgf("Using Redis Cluster: %s", address)
		store.clusterClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     address,
			Password:  password,
			TLSConfig: tlsConfig,
			ReadOnly:  true,
		})
		if (strings.ToLower(os.Getenv("ENABLE_DATADOG_REDIS_TRACING")) == "true") || (strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true") {
			redistrace.WrapClient(store.clusterClient, redistrace.WithServiceName(redisTraceServiceName))
			collector := redisprometheus.NewCollector("mlpfs", "redis", store.clusterClient)
			prometheus.MustRegister(collector)
		}
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
		return -1, fmt.Errorf("failed to convert redis_type to string: %+v", redisTypeJson)
	} else {
		if redisTypeStr == "redis" {
			t = redisNode
		} else if redisTypeStr == "redis_cluster" {
			t = redisCluster
		} else {
			return -1, fmt.Errorf("failed to convert redis_type to enum: %s. Must be one of 'redis', 'redis_cluster'", redisTypeStr)
		}
	}
	return t, nil
}

func (r *RedisOnlineStore) buildFeatureViewIndices(featureViewNames []string, featureNames []string) (map[string]int, map[int]string, int) {
	featureViewIndices := make(map[string]int)
	indicesFeatureView := make(map[int]string)
	index := len(featureNames)
	for _, featureViewName := range featureViewNames {
		if _, ok := featureViewIndices[featureViewName]; !ok {
			featureViewIndices[featureViewName] = index
			indicesFeatureView[index] = featureViewName
			index += 1
		}
	}
	return featureViewIndices, indicesFeatureView, index
}

func (r *RedisOnlineStore) buildRedisHashSetKeys(featureViewNames []string, featureNames []string, indicesFeatureView map[int]string, index int) ([]string, []string) {
	featureCount := len(featureNames)
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
	return hsetKeys, featureNames
}

func (r *RedisOnlineStore) buildRedisKeys(entityKeys []*types.EntityKey) ([]*[]byte, map[string]int, error) {
	redisKeys := make([]*[]byte, len(entityKeys))
	redisKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {
		var key, err = buildRedisKey(r.project, entityKeys[i], r.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, nil, err
		}
		redisKeys[i] = key
		redisKeyToEntityIndex[string(*key)] = i
	}
	return redisKeys, redisKeyToEntityIndex, nil
}

func (r *RedisOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "redis.OnlineRead")
	defer span.Finish()

	featureCount := len(featureNames)
	featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices(featureViewNames, featureNames)
	hsetKeys, featureNamesWithTimeStamps := r.buildRedisHashSetKeys(featureViewNames, featureNames, indicesFeatureView, index)
	redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
	if err != nil {
		return nil, err
	}

	results := make([][]FeatureData, len(entityKeys))
	commands := map[string]*redis.SliceCmd{}

	if r.t == redisNode {
		pipe := r.client.Pipeline()
		for _, redisKey := range redisKeys {
			keyString := string(*redisKey)
			commands[keyString] = pipe.HMGet(ctx, keyString, hsetKeys...)
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			return nil, err
		}
	} else if r.t == redisCluster {
		pipe := r.clusterClient.Pipeline()
		for _, redisKey := range redisKeys {
			keyString := string(*redisKey)
			commands[keyString] = pipe.HMGet(ctx, keyString, hsetKeys...)
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			return nil, err
		}
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
				featureName := featureNamesWithTimeStamps[featureIndex]
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
				var value *types.Value
				if value, _, err = UnmarshalStoredProto([]byte(valueString)); err != nil {
					return nil, errors.New("error converting parsed redis Value to types.Value")
				} else {
					featureName := featureNamesWithTimeStamps[featureIndex]
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

func (r *RedisOnlineStore) OnlineReadRange(ctx context.Context, groupedRefs *model.GroupedRangeFeatureRefs) ([][]RangeFeatureData, error) {
	// TODO: Implement OnlineReadRange
	return nil, errors.New("OnlineReadRange is not supported by RedisOnlineStore")
}

// Dummy destruct function to conform with plugin OnlineStore interface
func (r *RedisOnlineStore) Destruct() {

}

func buildRedisKey(project string, entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	serKey, err := utils.SerializeEntityKey(entityKey, entityKeySerializationVersion)
	if err != nil {
		return nil, err
	}
	fullKey := append(*serKey, []byte(project)...)
	return &fullKey, nil
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
		valueBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBuffer, uint64(x.Int64Val))
		return &valueBuffer, types.ValueType_INT64, nil
	case nil:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	default:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	}
}
