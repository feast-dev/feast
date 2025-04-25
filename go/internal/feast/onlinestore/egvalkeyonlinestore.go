package onlinestore

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/spaolacci/murmur3"
	valkey "github.com/valkey-io/valkey-go"

	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/rs/zerolog/log"
	// valkeytrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/valkey-go"
)

const defaultConnectionString = "localhost:6379"

type valkeyType int

const (
	valkeyNode    valkeyType = 0
	valkeyCluster valkeyType = 1
)

type ValkeyOnlineStore struct {

	// Feast project name
	project string

	// Valkey database type, either a single node server (ValkeyType.Valkey) or a cluster (ValkeyType.ValkeyCluster)
	t valkeyType

	// Valkey client connector
	client valkey.Client

	config *registry.RepoConfig
}

func parseConnectionString(onlineStoreConfig map[string]interface{}, valkeyStoreType valkeyType) (valkey.ClientOption, error) {
	var clientOption valkey.ClientOption

	clientOption.SendToReplicas = func(cmd valkey.Completed) bool {
		return cmd.IsReadOnly()
	}

	if valkeyStoreType == valkeyNode {
		replicaAddressJsonValue, ok := onlineStoreConfig["replica_address"]
		if !ok {
			log.Warn().Msg("define replica_address or reader endpoint to read from cluster replicas")
		} else {
			replicaAddress, ok := replicaAddressJsonValue.(string)
			if !ok {
				return clientOption, fmt.Errorf("failed to convert replica_address to string: %+v", replicaAddressJsonValue)
			}

			parts := strings.Split(replicaAddress, ",")
			for _, part := range parts {
				if strings.Contains(part, ":") {
					clientOption.Standalone.ReplicaAddress = append(clientOption.Standalone.ReplicaAddress, part)
				} else {
					return clientOption, fmt.Errorf("unable to parse part of replica_address: %s", part)
				}
			}
		}
	}

	valkeyConnJsonValue, ok := onlineStoreConfig["connection_string"]
	if !ok {
		valkeyConnJsonValue = defaultConnectionString
	}

	valkeyConnStr, ok := valkeyConnJsonValue.(string)
	if !ok {
		return clientOption, fmt.Errorf("failed to convert connection_string to string: %+v", valkeyConnJsonValue)
	}

	parts := strings.Split(valkeyConnStr, ",")
	for _, part := range parts {
		if strings.Contains(part, ":") {
			clientOption.InitAddress = append(clientOption.InitAddress, part)
		} else if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			switch kv[0] {
			case "password":
				clientOption.Password = kv[1]
			case "ssl":
				result, err := strconv.ParseBool(kv[1])
				if err != nil {
					return clientOption, err
				}
				if result {
					clientOption.TLSConfig = &tls.Config{}
				}
			case "db":
				db, err := strconv.Atoi(kv[1])
				if err != nil {
					return clientOption, err
				}
				clientOption.SelectDB = db
			default:
				return clientOption, fmt.Errorf("unrecognized option in connection_string: %s", kv[0])
			}
		} else {
			return clientOption, fmt.Errorf("unable to parse part of connection_string: %s", part)
		}
	}
	return clientOption, nil
}

func getValkeyTraceServiceName() string {
	datadogServiceName := os.Getenv("DD_SERVICE")
	var valkeyTraceServiceName string
	if datadogServiceName != "" {
		valkeyTraceServiceName = datadogServiceName + "-valkey"
	} else {
		valkeyTraceServiceName = "valkey.client" // Default service name
	}
	return valkeyTraceServiceName
}

func initializeValkeyClient(clientOption valkey.ClientOption, serviceName string) (valkey.Client, error) {
	if strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true" {
		// TODO: Configure once Datadog starts supporting valkey-go
		log.Warn().Msg("Valkey tracing is not enabled")
		// return valkeytrace.NewClient(clientOption, valkeytrace.WithServiceName(serviceName))
	}

	// TODO: Validate requests are routed to Replicas
	// without Reader endpoint specified for Standalone clusters

	return valkey.NewClient(clientOption)
}

func NewValkeyOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*ValkeyOnlineStore, error) {
	store := ValkeyOnlineStore{
		project: project,
		config:  config,
	}

	// Parse Valkey type
	valkeyStoreType, err := getValkeyType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}
	store.t = valkeyStoreType

	// Parse connection string
	clientOption, err := parseConnectionString(onlineStoreConfig, valkeyStoreType)
	if err != nil {
		return nil, err
	}

	// Initialize Valkey client
	store.client, err = initializeValkeyClient(clientOption, getValkeyTraceServiceName())
	if err != nil {
		return nil, err
	}

	log.Info().Msgf("Using Valkey: %s", clientOption.InitAddress)
	return &store, nil
}

func getValkeyType(onlineStoreConfig map[string]interface{}) (valkeyType, error) {
	var t valkeyType

	valkeyTypeJsonValue, ok := onlineStoreConfig["valkey_type"]
	if !ok {
		// Default to "valkey"
		valkeyTypeJsonValue = "valkey"
	} else if valkeyTypeStr, ok := valkeyTypeJsonValue.(string); !ok {
		return -1, fmt.Errorf("failed to convert valkey_type to string: %+v", valkeyTypeJsonValue)
	} else {
		if valkeyTypeStr == "valkey" {
			t = valkeyNode
		} else if valkeyTypeStr == "valkey_cluster" {
			t = valkeyCluster
		} else {
			return -1, fmt.Errorf("failed to convert valkey_type to enum: %s. Must be one of 'valkey', 'valkey_cluster'", valkeyTypeStr)
		}
	}
	return t, nil
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

func (r *ValkeyOnlineStore) buildHsetKeys(featureViewNames []string, featureNames []string, indicesFeatureView map[int]string, index int) ([]string, []string) {
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

func (r *ValkeyOnlineStore) buildValkeyKeys(entityKeys []*types.EntityKey) ([]*[]byte, error) {
	valkeyKeys := make([]*[]byte, len(entityKeys))
	for i := 0; i < len(entityKeys); i++ {
		var key, err = buildValkeyKey(r.project, entityKeys[i], r.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		valkeyKeys[i] = key
	}
	return valkeyKeys, nil
}

func (r *ValkeyOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "OnlineRead")
	defer span.Finish()

	featureCount := len(featureNames)
	featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices(featureViewNames, featureNames)
	hsetKeys, featureNamesWithTimeStamps := r.buildHsetKeys(featureViewNames, featureNames, indicesFeatureView, index)
	valkeyKeys, err := r.buildValkeyKeys(entityKeys)
	if err != nil {
		return nil, err
	}

	results := make([][]FeatureData, len(entityKeys))
	cmds := make(valkey.Commands, 0, len(entityKeys))

	for _, valkeyKey := range valkeyKeys {
		keyString := string(*valkeyKey)
		cmds = append(cmds, r.client.B().Hmget().Key(keyString).Field(hsetKeys...).Build())
	}

	var resContainsNonNil bool
	for entityIndex, values := range r.client.DoMulti(ctx, cmds...) {

		if err := values.Error(); err != nil {
			return nil, err
		}
		resContainsNonNil = false

		results[entityIndex] = make([]FeatureData, featureCount)

		res, err := values.ToArray()
		if err != nil {
			return nil, err
		}

		var timeStamp timestamppb.Timestamp
		var resString interface{}
		for featureIndex, featureValue := range res {
			resString = nil
			if !featureValue.IsNil() {
				resString, err = featureValue.ToString()
				if err != nil {
					return nil, err
				}
			}

			if featureIndex == featureCount {
				break
			}

			if resString == nil {
				// TODO (Ly): Can there be nil result within each feature or they will all be returned as string proto of types.Value_NullVal proto?
				featureName := featureNamesWithTimeStamps[featureIndex]
				featureViewName := featureViewNames[featureIndex]
				timeStampIndex := featureViewIndices[featureViewName]
				if !res[timeStampIndex].IsNil() {
					timeStampString, err := res[timeStampIndex].ToString()
					if err != nil {
						return nil, err
					}
					if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
						return nil, errors.New("error converting parsed valkey value to timestamppb.Timestamp")
					}
					if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
						return nil, errors.New("error converting parsed valkey value to timestamppb.Timestamp")
					}
					results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
						Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
						Value:     types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
					}
				} else {
					results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
						Timestamp: timestamppb.Timestamp{},
						Value:     types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
					}

				}

			} else if valueString, ok := resString.(string); !ok {
				return nil, errors.New("error parsing Value from valkey")
			} else {
				resContainsNonNil = true
				var value types.Value
				if err := proto.Unmarshal([]byte(valueString), &value); err != nil {
					return nil, errors.New("error converting parsed valkey Value to types.Value")
				} else {
					featureName := featureNamesWithTimeStamps[featureIndex]
					featureViewName := featureViewNames[featureIndex]
					timeStampIndex := featureViewIndices[featureViewName]
					timeStampString, err := res[timeStampIndex].ToString()
					if err != nil {
						return nil, err
					}
					if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
						return nil, errors.New("error converting parsed valkey Value to timestamppb.Timestamp")
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

func (r *ValkeyOnlineStore) OnlineReadRange(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string, sortKeyFilters []*model.SortKeyFilter, limit int32) ([][]RangeFeatureData, error) {
	// TODO: Implement OnlineReadRange
	return nil, errors.New("OnlineReadRange is not supported by ValkeyOnlineStore")
}

// Dummy destruct function to conform with plugin OnlineStore interface
func (r *ValkeyOnlineStore) Destruct() {

}

func buildValkeyKey(project string, entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	serKey, err := utils.SerializeEntityKey(entityKey, entityKeySerializationVersion)
	if err != nil {
		return nil, err
	}
	fullKey := append(*serKey, []byte(project)...)
	return &fullKey, nil
}
