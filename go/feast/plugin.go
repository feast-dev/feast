package feast

import (
	"context"
	"github.com/hashicorp/go-plugin"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/protos/feast/third_party/grpc/connector"
	"google.golang.org/grpc"
	// "net/rpc"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	// This isn't required when using VersionedPlugins
	ProtocolVersion:  1,
	MagicCookieKey:   "BASIC_PLUGIN",
	MagicCookieValue: "hello",
}

// PluginMap is the map of plugins we can dispense.
var PluginMap = map[string]plugin.Plugin{
	"onlinestore_grpc": &OnlineStoreGRPCPlugin{},
}

// // // This is the implementation of plugin.GRPCPlugin so we can serve/consume this.
type OnlineStoreGRPCPlugin struct {
	// GRPCPlugin must still implement the Plugin interface
	plugin.Plugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl OnlineStore
}

// GRPCClient is an implementation of KV that talks over RPC.
type GRPCClient struct{ client connector.OnlineStoreClient }

func (m *GRPCClient) OnlineRead(entityKeys []types.EntityKey, view string, features []string) ([][]Feature, error) {
	entityKeysRef := make([]*types.EntityKey, len(entityKeys))
	for i := 0; i < len(entityKeys); i++ {
		entityKeysRef[i] = &entityKeys[i]
	}
	results, err := m.client.OnlineRead(context.Background(), &connector.OnlineReadRequest{
		EntityKeys:   entityKeysRef,
		View: view,
		Features: features,
	})
	if err != nil {
		return nil, err
	}
	feature2D := results.GetResults()
	featureResults := make([][]Feature, len(feature2D))
	for entityIndex, featureList := range feature2D {
		connectorList := featureList.GetFeatureList()
		featureResults[entityIndex] = make([]Feature, len(connectorList))
		for featureIndex, feature := range connectorList {
			featureResults[entityIndex][featureIndex] = Feature{ 	reference: *feature.GetReference(),
																	timestamp: *feature.GetTimestamp(),
																	value: *feature.GetValue() }
		}
	}
	return featureResults, nil
}

func (p *OnlineStoreGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &GRPCClient{client: connector.NewOnlineStoreClient(c)}, nil
}