// Package tui is the Bubbletea observer: a read-only terminal view of a
// running daemon's channels and messages. It talks to the daemon over the
// HTTP API and SSE stream (HTTPBackend), so it can be attached to and
// detached from a daemon at will; it never runs an engine of its own.
// Control actions (start/stop, replay, script editing) belong to the web
// UI.
package tui

import (
	"context"

	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
)

// Backend is what the TUI needs from a daemon: the same view models the
// HTTP API serves. HTTPBackend implements it over the API; tests use a
// fake.
type Backend interface {
	// ChannelSummaries lists channels with counts, the same view model the
	// web dashboard and JSON API use.
	ChannelSummaries(ctx context.Context) ([]engine.ChannelSummary, error)
	ChannelSummary(ctx context.Context, channelID string) (engine.ChannelSummary, error)
	ListMessages(ctx context.Context, channelID string, q store.ListQuery) ([]store.MessageSummary, error)
	GetMessage(ctx context.Context, id int64) (*store.MessageDetail, error)
	MessageTree(ctx context.Context, id int64, stage string) (*message.Node, string, error)
	MessageDiff(ctx context.Context, id int64, destID string) ([]message.DiffEntry, error)
	// Subscribe delivers the daemon's event stream; the returned func ends
	// the subscription and closes the channel.
	Subscribe(buf int) (<-chan events.Event, func())
}
