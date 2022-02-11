package feast

import (
	"errors"
	"fmt"
	// "io/ioutil"
	// "log"
	"os"
	"os/exec"
	"github.com/hashicorp/go-plugin"
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
		fmt.Println("Hello world")
		return connectorClient(KV_PLUGIN)
	}
}

func connectorClient(KV_PLUGIN string) (OnlineStore, error) {
	// We don't want to see the plugin logs.
	// log.SetOutput(ioutil.Discard)

	// We're a host. Start by launching the plugin process.
	cmd := exec.Command("sh", "-c", KV_PLUGIN )
	cmd.Env = os.Environ()
    cmd.Env = append(cmd.Env, "PYTHONPATH=test_repo/connector_python:$PYTHONPATH")

	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: Handshake,
		Plugins:         PluginMap,
		Cmd:             cmd,
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC},
	})
	// defer client.Kill()
	// Connect via RPC
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}
	fmt.Println("here")

	// Request the plugin
	raw, err := rpcClient.Dispense("onlinestore_grpc")
	if err != nil {
		return nil, err
	}
	fmt.Println("here 2")

	// We should have a OnlineStore now! This feels like a normal interface
	// implementation but is in fact over an RPC connection.
	if onlineStore, ok := raw.(OnlineStore); !ok {
		return nil, errors.New("Error creating a Connector OnlineStore")
	} else {
		return onlineStore, nil
	}
}
