package buildinfo

import "runtime"

// These values are intended to be replaced with -ldflags at build time.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

// Info is the immutable build metadata exposed by logs, health, and metrics.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
}

// Current returns the build metadata for the running binary.
func Current() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildTime: BuildTime,
		GoVersion: runtime.Version(),
	}
}
