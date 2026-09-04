package adapter

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDurationUnmarshal(t *testing.T) {
	cases := []struct {
		yaml    string
		want    time.Duration
		wantErr string
	}{
		{yaml: `d: 30s`, want: 30 * time.Second},
		{yaml: `d: "1m30s"`, want: 90 * time.Second},
		{yaml: `d: 250ms`, want: 250 * time.Millisecond},
		{yaml: `d: 0s`, want: 0},
		// A bare number used to mean nanoseconds: holdTimeout: 30 was 30ns.
		{yaml: `d: 30`, wantErr: "unit is required"},
		{yaml: `d: 1.5`, wantErr: "unit is required"},
		{yaml: `d: -5s`, wantErr: "negative"},
		{yaml: `d: soon`, wantErr: "invalid duration"},
	}
	for _, tc := range cases {
		t.Run(tc.yaml, func(t *testing.T) {
			var out struct {
				D Duration `yaml:"d"`
			}
			err := yaml.Unmarshal([]byte(tc.yaml), &out)
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
			case err != nil:
				t.Fatal(err)
			case time.Duration(out.D) != tc.want:
				t.Fatalf("got %v, want %v", time.Duration(out.D), tc.want)
			}
		})
	}
}
