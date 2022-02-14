package feast

import (
	"errors"
	"fmt"
	// "io/ioutil"
	// "log"
	"os"
	"os/exec"
	"github.com/hashicorp/go-plugin"
	"github.com/hashicorp/go-hclog"
	"path/filepath"
	"runtime"
	// "time"
	// "github.com/feast-dev/feast/go/protos/feast/third_party/grpc/connector"
)

func getOnlineStore(config *RepoConfig) (OnlineStore, error) {
	onlineStoreType, ok := getOnlineStoreType(config.OnlineStore)
	if !ok {
		return nil, errors.New(fmt.Sprintf("could not get online store type from online store config: %+v", config.OnlineStore))
	}
	if onlineStoreType == "redis" {
		onlineStore, err := NewRedisOnlineStore(config.Project, config.OnlineStore)
		return onlineStore, err
	} else {
		// TODO(willem): Python connectors here
		KV_PLUGIN := config.OnlineStore["KV_PLUGIN"].(string)
		return connectorClient(KV_PLUGIN)
	}
}

func connectorClient(KV_PLUGIN string) (OnlineStore, error) {
	// We don't want to see the plugin logs.
	// log.SetOutput(ioutil.Discard)

	// We're a host. Start by launching the plugin process.
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("couldn't find file path of the connector file")
	}
	cmd := exec.Command("sh", "-c", KV_PLUGIN )
	cmd.Env = os.Environ()
	connectorPythonPath := filepath.Join(filename, "..", "..", "test_repo/connector_python")
    cmd.Env = append(cmd.Env, fmt.Sprintf("PYTHONPATH=%s:$PYTHONPATH", connectorPythonPath))

	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "plugin",
		Output: os.Stdout,
		Level:  hclog.Debug,
	})

	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins:         PluginMap,
		Cmd:             cmd,
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC},
		Logger:          logger,
	})
	
	// Connect via RPC
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}
	// Request the plugin
	raw, err := rpcClient.Dispense("onlinestore_grpc")
	if err != nil {
		return nil, err
	}

	// We should have a OnlineStore now! This feels like a normal interface
	// implementation but is in fact over an RPC connection.
	if onlineStore, ok := raw.(OnlineStore); !ok {
		return nil, errors.New("Error creating a Connector OnlineStore")
	} else {
		grpcClient, ok := onlineStore.(*GRPCClient)
		if !ok {
			return nil, errors.New("Connector is not a *connector.GrpcClient")
		}
		grpcClient.destructor = func() {
			client.Kill()
		}
		return onlineStore, nil
	}
	// return raw, nil
}
