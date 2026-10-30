package version

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func vcs(rev, time, modified string) []debug.BuildSetting {
	return []debug.BuildSetting{
		{Key: "vcs", Value: "git"},
		{Key: "vcs.revision", Value: rev},
		{Key: "vcs.time", Value: time},
		{Key: "vcs.modified", Value: modified},
	}
}

const rev = "016fe9b6bff306a9f60228f903c144295ac8878f"

func TestFromBuildInfo(t *testing.T) {
	cases := []struct {
		name string
		bi   *debug.BuildInfo
		want Info
		str  string
	}{
		{
			name: "tagged release",
			bi: &debug.BuildInfo{
				GoVersion: "go1.27.3",
				Main:      debug.Module{Version: "v0.1.0"},
				Settings:  vcs(rev, "2026-10-30T05:34:40Z", "false"),
			},
			want: Info{Version: "v0.1.0", Revision: rev, Time: "2026-10-30T05:34:40Z", GoVersion: "go1.27.3"},
			str:  "v0.1.0 (commit 016fe9b6bff3, 2026-10-30T05:34:40Z, go1.27.3)",
		},
		{
			name: "commit after a tag, uncommitted changes",
			bi: &debug.BuildInfo{
				GoVersion: "go1.27.3",
				Main:      debug.Module{Version: "v0.1.1-0.20261030053440-016fe9b6bff3+dirty"},
				Settings:  vcs(rev, "2026-10-30T05:34:40Z", "true"),
			},
			want: Info{
				Version: "v0.1.1-0.20261030053440-016fe9b6bff3+dirty", Revision: rev,
				Time: "2026-10-30T05:34:40Z", Modified: true, GoVersion: "go1.27.3",
			},
			str: "v0.1.1-0.20261030053440-016fe9b6bff3+dirty (commit 016fe9b6bff3, 2026-10-30T05:34:40Z, go1.27.3)",
		},
		{
			name: "no version control information",
			bi:   &debug.BuildInfo{GoVersion: "go1.27.3", Main: debug.Module{Version: Devel}},
			want: Info{Version: Devel, GoVersion: "go1.27.3"},
			str:  "(devel) (go1.27.3)",
		},
		{
			name: "empty main version",
			bi:   &debug.BuildInfo{GoVersion: "go1.27.3"},
			want: Info{Version: Devel, GoVersion: "go1.27.3"},
			str:  "(devel) (go1.27.3)",
		},
		{
			name: "no build information",
			want: Info{Version: Devel, GoVersion: runtime.Version()},
			str:  "(devel) (" + runtime.Version() + ")",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fromBuildInfo(tc.bi)
			if got != tc.want {
				t.Errorf("fromBuildInfo = %+v, want %+v", got, tc.want)
			}
			if s := got.String(); s != tc.str {
				t.Errorf("String() = %q, want %q", s, tc.str)
			}
		})
	}
}

func TestGetReadsTheRunningBinary(t *testing.T) {
	got := Get()
	if got.Version == "" {
		t.Error("Version is empty")
	}
	if got.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", got.GoVersion, runtime.Version())
	}
}
