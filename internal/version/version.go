// Package version reports which build of waggle is running. Nothing is set
// at release time: the Go toolchain stamps every binary built from a git
// checkout with the module version and the commit. A build at the v0.1.0
// tag reports v0.1.0, a build of any other commit reports a pseudo-version
// naming it (v0.1.1-0.20261030120000-abcdef123456), and uncommitted changes
// add +dirty. A binary built without version control information (from a
// source archive, or with -buildvcs=false) reports (devel).
package version

import (
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

// Devel is the version of a build that carried no version control
// information.
const Devel = "(devel)"

// Info describes the running binary.
type Info struct {
	// Version is the module version, or Devel.
	Version string `json:"version"`
	// Revision is the full commit hash the binary was built from.
	Revision string `json:"revision,omitempty"`
	// Time is the commit time, RFC 3339.
	Time string `json:"time,omitempty"`
	// Modified reports uncommitted changes in the checkout it was built from.
	Modified bool `json:"modified,omitempty"`
	// GoVersion is the toolchain that built the binary.
	GoVersion string `json:"goVersion"`
}

// Get returns the running binary's build information. It is read once.
var Get = sync.OnceValue(func() Info {
	bi, _ := debug.ReadBuildInfo()
	return fromBuildInfo(bi)
})

func fromBuildInfo(bi *debug.BuildInfo) Info {
	info := Info{Version: Devel, GoVersion: runtime.Version()}
	if bi == nil {
		return info
	}
	if bi.GoVersion != "" {
		info.GoVersion = bi.GoVersion
	}
	if bi.Main.Version != "" {
		info.Version = bi.Main.Version
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			info.Revision = s.Value
		case "vcs.time":
			info.Time = s.Value
		case "vcs.modified":
			info.Modified = s.Value == "true"
		}
	}
	return info
}

// String is the one-line form `waggle version` prints, for example
// "v0.1.0 (commit 016fe9b6bff3, 2026-10-30T05:34:40Z, go1.27.3)".
func (i Info) String() string {
	var details []string
	if i.Revision != "" {
		details = append(details, "commit "+shortRevision(i.Revision))
	}
	if i.Time != "" {
		details = append(details, i.Time)
	}
	details = append(details, i.GoVersion)
	return i.Version + " (" + strings.Join(details, ", ") + ")"
}

// shortRevision abbreviates a commit hash to the 12 characters Go uses in
// pseudo-versions.
func shortRevision(rev string) string {
	if len(rev) > 12 {
		return rev[:12]
	}
	return rev
}
