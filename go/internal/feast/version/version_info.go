package version

import (
	"runtime"
)

var (
	Version    = "dev"
	BuildTime  = "unknown"
	CommitHash = "none"
	GoVersion  = runtime.Version()
	ServerType = "none"
)

type Info struct {
	Version    string `json:"version"`
	BuildTime  string `json:"build_time"`
	CommitHash string `json:"commit_hash"`
	GoVersion  string `json:"go_version"`
	ServerType string `json:"server_type"`
}

func GetVersionInfo() *Info {
	return &Info{
		Version:    Version,
		BuildTime:  BuildTime,
		CommitHash: CommitHash,
		GoVersion:  GoVersion,
		ServerType: ServerType,
	}
}
