package onlinestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/golang/protobuf/ptypes/timestamp"
)

type FeatureData struct {
	Reference serving.FeatureReferenceV2
	Timestamp timestamp.Timestamp
	Value     types.Value
}

type OnlineStore interface {
	// OnlineRead reads multiple features (specified in featureReferences) for multiple
	// entity keys (specified in entityKeys) and returns an array of array of features,
	// where each feature contains 3 fields:
	//   1. feature Reference
	//   2. feature event timestamp
	//   3. feature value
	// The inner array will have the same size as featureReferences,
	// while the outer array will have the same size as entityKeys.

	// TODO: Can we return [][]FeatureData, []timstamps, error
	// instead and remove timestamp from FeatureData struct to mimic Python's code
	// and reduces repeated memory storage for the same timstamp (which is stored as value and not as a pointer).
	// Should each attribute in FeatureData be stored as a pointer instead since the current
	// design forces value copied in OnlineRead + GetOnlineFeatures
	// (array is destructed so we cannot use the same fields in each
	// Feature object as pointers in GetOnlineFeaturesResponse)
	// => allocate memory for each field once in OnlineRead
	// and reuse them in GetOnlineFeaturesResponse?
	OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error)
	// Destruct must be call once user is done using OnlineStore
	// This is to comply with the Connector since we have to close the plugin
	Destruct()
}

func getOnlineStoreType(onlineStoreConfig map[string]interface{}) (string, bool) {
	if onlineStoreType, ok := onlineStoreConfig["type"]; !ok {
		// If online store type isn't specified, default to sqlite
		return "sqlite", true
	} else {
		result, ok := onlineStoreType.(string)
		return result, ok
	}
}

func NewOnlineStore(config *registry.RepoConfig) (OnlineStore, error) {
	onlineStoreType, ok := getOnlineStoreType(config.OnlineStore)
	if !ok {
		return nil, fmt.Errorf("could not get online store type from online store config: %+v", config.OnlineStore)
	} else if onlineStoreType == "sqlite" {
		onlineStore, err := NewSqliteOnlineStore(config.Project, config, config.OnlineStore)
		return onlineStore, err
	} else if onlineStoreType == "redis" {
		onlineStore, err := NewRedisOnlineStore(config.Project, config, config.OnlineStore)
		return onlineStore, err
	} else if onlineStoreType == "dynamodb" {
		onlineStore, err := NewDynamodbOnlineStore(config.Project, config, config.OnlineStore)
		return onlineStore, err
	} else {
		return nil, fmt.Errorf("%s online store type is currently not supported; only redis and sqlite are supported", onlineStoreType)
	}
}

func serializeEntityKey(entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	// Serialize entity key to a bytestring so that it can be used as a lookup key in a hash table.

	// Ensure that we have the right amount of join keys and entity values
	if len(entityKey.JoinKeys) != len(entityKey.EntityValues) {
		return nil, fmt.Errorf("the amount of join key names and entity values don't match: %s vs %s", entityKey.JoinKeys, entityKey.EntityValues)
	}

	// Make sure that join keys are sorted so that we have consistent key building
	m := make(map[string]*types.Value)

	for i := 0; i < len(entityKey.JoinKeys); i++ {
		m[entityKey.JoinKeys[i]] = entityKey.EntityValues[i]
	}

	keys := make([]string, 0, len(m))
	keys = append(keys, entityKey.JoinKeys...)
	sort.Strings(keys)  // Sort the keys

	// Build the key
	length := 7 * len(keys)
	bufferList := make([][]byte, length)
	offset := 0

	// For entityKeySerializationVersion 3 and above, we add the number of join keys
	// as the first 4 bytes of the serialized key.
	if entityKeySerializationVersion >= 3 {
		byteBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(byteBuffer, uint32(len(keys)))
		bufferList[offset] = byteBuffer  // First buffer is always the length of the keys
		offset++
	}

	for i := 0; i < len(keys); i++ {
		// Add the key type STRING info
		byteBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(byteBuffer, uint32(types.ValueType_Enum_value["STRING"]))
		bufferList[offset] = byteBuffer
		offset++

		// Add the size of current "key" string
		keyLenByteBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyLenByteBuffer, uint32(len(keys[i])))
		bufferList[offset] = keyLenByteBuffer
		offset++

		// Add value
		bufferList[offset] = []byte(keys[i])
		offset++
	}

	for i := 0; i < len(keys); i++ {
		value := m[keys[i]].GetVal()

		valueBytes, valueTypeBytes, err := serializeValue(value, entityKeySerializationVersion)
		if err != nil {
			return valueBytes, err
		}

		// Add value type info
		typeBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(typeBuffer, uint32(valueTypeBytes))
		bufferList[offset] = typeBuffer
		offset++

		// Add length info
		lenBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuffer, uint32(len(*valueBytes)))
		bufferList[offset] = lenBuffer
		offset++

		bufferList[offset] = *valueBytes
		offset++
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
		valueBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBuffer, uint64(x.Int64Val))
		return &valueBuffer, types.ValueType_INT64, nil
	case nil:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	default:
		return nil, types.ValueType_INVALID, fmt.Errorf("could not detect type for %v", x)
	}
}
