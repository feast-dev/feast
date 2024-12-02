package utils

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"sort"
)

func HashSerializedEntityKey(serializedEntityKey *[]byte) string {
	if serializedEntityKey == nil {
		return ""
	}
	h := sha1.New()
	h.Write(*serializedEntityKey)
	return hex.EncodeToString(h.Sum(nil))
}

// SerializeEntityKey Serialize entity key to a bytestring so that it can be used as a lookup key in a hash table.
func SerializeEntityKey(entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
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
