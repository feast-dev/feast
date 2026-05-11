package onlinestore

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	valkey "github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/rs/zerolog/log"
)

type ValkeyOnlineStore struct {

	// Feast project name
	project string

	// Valkey connection type, either a single node server (redisNode) or a cluster (redisCluster)
	t redisType

	// Valkey client (handles both standalone and cluster modes)
	client valkey.Client

	// Parsed connection options (stored for introspection/testing)
	opts valkey.ClientOption

	config *registry.RepoConfig
}

func NewValkeyOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*ValkeyOnlineStore, error) {
	store := ValkeyOnlineStore{
		project: project,
		config:  config,
	}

	var address []string
	var username string
	var password string
	var tlsConfig *tls.Config
	var db int // Default to 0

	valkeyStoreType, err := getRedisType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}
	store.t = valkeyStoreType

	// Parse connection_string and write it into conf.address, conf.password, and conf.ssl
	valkeyConnJson, ok := onlineStoreConfig["connection_string"]
	if !ok {
		// Default to "localhost:6379"
		valkeyConnJson = "localhost:6379"
	}
	if valkeyConnStr, ok := valkeyConnJson.(string); !ok {
		return nil, fmt.Errorf("failed to convert connection_string to string: %+v", valkeyConnJson)
	} else {
		parts := strings.Split(valkeyConnStr, ",")
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

	clientOption := valkey.ClientOption{
		InitAddress: address,
		Username:    username,
		Password:    password,
		TLSConfig:   tlsConfig,
	}

	if valkeyStoreType == redisNode {
		log.Info().Msgf("Using Valkey: %s", address[0])
		clientOption.InitAddress = address[:1]
		clientOption.SelectDB = db
	} else {
		log.Info().Msgf("Using Valkey Cluster: %s", address)
		clientOption.ReplicaOnly = true
	}

	store.opts = clientOption

	client, err := valkey.NewClient(clientOption)
	if err != nil {
		return nil, err
	}
	store.client = client

	return &store, nil
}

func (r *ValkeyOnlineStore) buildFeatureViewIndices(featureViewNames []string, featureNames []string) (map[string]int, map[int]string, int) {
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

func (r *ValkeyOnlineStore) buildValkeyKeys(entityKeys []*types.EntityKey) ([]*[]byte, map[string]int, error) {
	valkeyKeys := make([]*[]byte, len(entityKeys))
	valkeyKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {
		var key, err = buildValkeyKey(r.project, entityKeys[i], r.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, nil, err
		}
		valkeyKeys[i] = key
		valkeyKeyToEntityIndex[string(*key)] = i
	}
	return valkeyKeys, valkeyKeyToEntityIndex, nil
}

func (r *ValkeyOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	ctx, span := tracer.Start(ctx, "valkey.OnlineRead")
	defer span.End()

	featureCount := len(featureNames)
	featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices(featureViewNames, featureNames)
	hsetKeys, featureNamesWithTimeStamps := buildRedisHashSetKeys(featureViewNames, featureNames, indicesFeatureView, index)
	valkeyKeys, valkeyKeyToEntityIndex, err := r.buildValkeyKeys(entityKeys)
	if err != nil {
		return nil, err
	}

	results := make([][]FeatureData, len(entityKeys))

	// Build pipelined HMGET commands in key order so results can be correlated by index.
	cmds := make([]valkey.Completed, len(valkeyKeys))
	keyOrder := make([]string, len(valkeyKeys))
	for i, valkeyKey := range valkeyKeys {
		keyString := string(*valkeyKey)
		keyOrder[i] = keyString
		cmds[i] = r.client.B().Hmget().Key(keyString).Field(hsetKeys...).Build()
	}

	responses := r.client.DoMulti(ctx, cmds...)

	for i, resp := range responses {
		keyString := keyOrder[i]
		entityIndex := valkeyKeyToEntityIndex[keyString]

		results[entityIndex] = make([]FeatureData, featureCount)

		arr, err := resp.ToArray()
		if err != nil {
			return nil, err
		}

		var timeStamp timestamppb.Timestamp
		resContainsNonNil := false

		for featureIndex, msg := range arr {
			if featureIndex == featureCount {
				break
			}

			if msg.IsNil() {
				// TODO (Ly): Can there be nil result within each feature or they will all be returned as string proto of types.Value_NullVal proto?
				featureName := featureNamesWithTimeStamps[featureIndex]
				featureViewName := featureViewNames[featureIndex]
				timeStampIndex := featureViewIndices[featureViewName]
				tsMsg := arr[timeStampIndex]
				if !tsMsg.IsNil() {
					timeStampString, err := tsMsg.ToString()
					if err != nil {
						return nil, errors.New("error parsing value from valkey")
					}
					if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
						return nil, errors.New("error converting parsed valkey value to timestamppb.Timestamp")
					}
				}

				results[entityIndex][featureIndex] = FeatureData{
					Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
					Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
					Value:     types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
				}
			} else {
				valueString, err := msg.ToString()
				if err != nil {
					return nil, errors.New("error parsing Value from valkey")
				}
				resContainsNonNil = true
				var value types.Value
				if err := proto.Unmarshal([]byte(valueString), &value); err != nil {
					return nil, errors.New("error converting parsed valkey Value to types.Value")
				}
				featureName := featureNamesWithTimeStamps[featureIndex]
				featureViewName := featureViewNames[featureIndex]
				timeStampIndex := featureViewIndices[featureViewName]
				tsMsg := arr[timeStampIndex]
				if !tsMsg.IsNil() {
					timeStampString, err := tsMsg.ToString()
					if err != nil {
						return nil, errors.New("error parsing Value from valkey")
					}
					if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
						return nil, errors.New("error converting parsed valkey Value to timestamppb.Timestamp")
					}
				}
				results[entityIndex][featureIndex] = FeatureData{
					Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
					Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
					Value:     types.Value{Val: value.Val},
				}
			}
		}

		if !resContainsNonNil {
			results[entityIndex] = nil
		}
	}

	return results, nil
}

func (r *ValkeyOnlineStore) Destruct() {
	r.client.Close()
}

func buildValkeyKey(project string, entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	serKey, err := serializeEntityKey(entityKey, entityKeySerializationVersion)
	if err != nil {
		return nil, err
	}
	fullKey := append(*serKey, []byte(project)...)
	return &fullKey, nil
}
