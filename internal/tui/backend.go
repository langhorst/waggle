// Package tui is the Bubbletea observer: it embeds the engine in-process
// (no HTTP hop) and renders live channel/message state read-only — a
// development and testing companion, not a control surface. Control actions
// (start/stop, replay, script editing) belong to the web UI.
package tui

import (
	"context"

	"github.com/langhorst/waggle/internal/engine"
	"github.com/langhorst/waggle/internal/events"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
)

// Backend is what the TUI needs from the engine — a seam so model tests run
// against a fake. Because the TUI talks to the same surface the HTTP layer
// wraps, a TUI-over-API variant can implement this later without touching
// the views.
type Backend interface {
	Channels() []engine.Info
	ListMessages(ctx context.Context, channelID string, q store.ListQuery) ([]store.MessageSummary, error)
	GetMessage(ctx context.Context, id int64) (*store.MessageDetail, error)
	MessageCounts(ctx context.Context, channelID string) (map[message.State]int, error)
	QueueDepth(ctx context.Context, channelID string) (map[string]int, error)
	MessageTree(ctx context.Context, id int64, stage string) (*message.Node, string, error)
	MessageDiff(ctx context.Context, id int64, destID string) ([]message.DiffEntry, error)
	Subscribe(buf int) (<-chan events.Event, func())
}

// EngineBackend adapts an embedded *engine.Engine (with persistence) to the
// Backend seam.
type EngineBackend struct {
	Eng *engine.Engine
}

func (b EngineBackend) Channels() []engine.Info { return b.Eng.Channels() }

func (b EngineBackend) ListMessages(ctx context.Context, channelID string, q store.ListQuery) ([]store.MessageSummary, error) {
	return b.Eng.Store().ListMessages(ctx, channelID, q)
}

func (b EngineBackend) GetMessage(ctx context.Context, id int64) (*store.MessageDetail, error) {
	return b.Eng.Store().GetMessage(ctx, id)
}

func (b EngineBackend) MessageCounts(ctx context.Context, channelID string) (map[message.State]int, error) {
	return b.Eng.Store().MessageCounts(ctx, channelID)
}

func (b EngineBackend) QueueDepth(ctx context.Context, channelID string) (map[string]int, error) {
	return b.Eng.Store().QueueDepth(ctx, channelID)
}

func (b EngineBackend) MessageTree(ctx context.Context, id int64, stage string) (*message.Node, string, error) {
	return b.Eng.MessageTree(ctx, id, stage)
}

func (b EngineBackend) MessageDiff(ctx context.Context, id int64, destID string) ([]message.DiffEntry, error) {
	return b.Eng.MessageDiff(ctx, id, destID)
}

func (b EngineBackend) Subscribe(buf int) (<-chan events.Event, func()) {
	return b.Eng.Bus().Subscribe(buf)
}
