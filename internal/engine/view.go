package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
	"github.com/langhorst/waggle/internal/store"
)

// Stage selects which representation of a stored message to inspect:
// "received" (raw inbound), "transformed" (after the channel translator
// chain), or "dest:<destinationID>" (the payload serialized for one
// destination).
const (
	StageReceived    = "received"
	StageTransformed = "transformed"
)

// MessageTree parses the selected stage of a stored message into its tree
// for the explorer views. Returns the tree and the data type it was parsed
// with.
func (e *Engine) MessageTree(ctx context.Context, id int64, stage string) (*message.Node, string, error) {
	raw, dtName, err := e.stagePayload(ctx, id, stage)
	if err != nil {
		return nil, "", err
	}
	dt, ok := format.Get(dtName)
	if !ok {
		return nil, "", fmt.Errorf("engine: unknown data type %q", dtName)
	}
	tree, err := dt.Parse(raw)
	if err != nil {
		return nil, "", fmt.Errorf("engine: parsing %s payload: %w", stage, err)
	}
	return tree, dtName, nil
}

// MessageDiff computes the structural diff between what a channel received
// and what it produced: the transformed payload by default, or one
// destination's serialized payload when destID is set. Paths render in each
// side's dialect, so a same-format diff reads like "PID-5.1: DOE → SMITH"
// and a cross-format diff lists both flattened sides.
func (e *Engine) MessageDiff(ctx context.Context, id int64, destID string) ([]message.DiffEntry, error) {
	fromTree, fromDT, err := e.MessageTree(ctx, id, StageReceived)
	if err != nil {
		return nil, err
	}
	stage := StageTransformed
	if destID != "" {
		stage = "dest:" + destID
	}
	toTree, toDT, err := e.MessageTree(ctx, id, stage)
	if err != nil {
		return nil, err
	}
	from, _ := format.Get(fromDT)
	to, _ := format.Get(toDT)
	return message.Diff(from.Flatten(fromTree), to.Flatten(toTree)), nil
}

func (e *Engine) stagePayload(ctx context.Context, id int64, stage string) ([]byte, string, error) {
	if e.store == nil {
		return nil, "", fmt.Errorf("engine: message inspection requires persistence")
	}
	detail, err := e.store.GetMessage(ctx, id)
	if err != nil {
		return nil, "", err
	}
	switch {
	case stage == "" || stage == StageReceived:
		return detail.Raw, detail.DataType, nil

	case stage == StageTransformed:
		if len(detail.Transformed) == 0 {
			return nil, "", fmt.Errorf("engine: message %d has no transformed payload (state %s)", id, detail.State)
		}
		dtName := detail.TransformedDataType
		if dtName == "" {
			dtName = detail.DataType
		}
		return detail.Transformed, dtName, nil

	case strings.HasPrefix(stage, "dest:"):
		destID := strings.TrimPrefix(stage, "dest:")
		payload, err := e.store.DestinationPayload(ctx, id, destID)
		if err != nil {
			return nil, "", err
		}
		if len(payload) == 0 {
			return nil, "", fmt.Errorf("engine: no payload stored for message %d destination %s", id, destID)
		}
		dtName, err := e.destinationDataType(detail.ChannelID, destID)
		if err != nil {
			return nil, "", err
		}
		return payload, dtName, nil

	default:
		return nil, "", fmt.Errorf("engine: unknown stage %q (expected received, transformed, or dest:<id>)", stage)
	}
}

func (e *Engine) destinationDataType(channelID, destID string) (string, error) {
	cfg, ok := e.Config(channelID)
	if !ok {
		return "", fmt.Errorf("engine: unknown channel %q", channelID)
	}
	for _, d := range cfg.Destinations {
		if d.ID == destID {
			return d.DataType, nil
		}
	}
	return "", fmt.Errorf("engine: channel %s has no destination %q", channelID, destID)
}

// ErrUnknownChannel is returned for an id no loaded channel has.
var ErrUnknownChannel = errors.New("engine: unknown channel")

// ErrUnknownAction is returned by Lifecycle for an action it does not know.
var ErrUnknownAction = errors.New("engine: unknown action")

// ChannelSummary is one channel as the UIs list it: identity, status, and
// (with persistence) per-state message counts and queue depth per
// destination. It is the one view model behind the JSON list, the web
// dashboard, and the TUI channel table.
type ChannelSummary struct {
	Info
	Counts     map[message.State]int `json:"counts,omitempty"`
	QueueDepth map[string]int        `json:"queueDepth,omitempty"`
}

// Received counts messages the channel accepted (recorded or already
// transformed; delivery outcomes are per destination).
func (s ChannelSummary) Received() int {
	return s.Counts[message.StateReceived] + s.Counts[message.StateTransformed]
}

// Sent counts destination deliveries that succeeded.
func (s ChannelSummary) Sent() int { return s.Counts[message.StateSent] }

// Errors counts pipeline errors plus dead-lettered deliveries.
func (s ChannelSummary) Errors() int { return s.Counts[message.StateError] }

// Filtered counts messages a Message Filter dropped.
func (s ChannelSummary) Filtered() int { return s.Counts[message.StateFiltered] }

// Queued is the total pending deliveries across destinations.
func (s ChannelSummary) Queued() int {
	total := 0
	for _, n := range s.QueueDepth {
		total += n
	}
	return total
}

// ChannelSummaries lists every channel in configuration order with its
// counts. Without a store the counts are absent.
func (e *Engine) ChannelSummaries(ctx context.Context) ([]ChannelSummary, error) {
	infos := e.Channels()
	out := make([]ChannelSummary, 0, len(infos))
	for _, info := range infos {
		s, err := e.summarize(ctx, info)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// ChannelSummary is ChannelSummaries for one channel.
func (e *Engine) ChannelSummary(ctx context.Context, id string) (ChannelSummary, error) {
	m, err := e.managed(id)
	if err != nil {
		return ChannelSummary{}, err
	}
	return e.summarize(ctx, Info{ID: m.cfg.ID, Name: m.cfg.Name, Status: m.ch.Status()})
}

func (e *Engine) summarize(ctx context.Context, info Info) (ChannelSummary, error) {
	s := ChannelSummary{Info: info}
	if e.store == nil {
		return s, nil
	}
	counts, err := e.store.MessageCounts(ctx, info.ID)
	if err != nil {
		return s, err
	}
	depth, err := e.store.QueueDepth(ctx, info.ID)
	if err != nil {
		return s, err
	}
	s.Counts, s.QueueDepth = counts, depth
	return s, nil
}

// Lifecycle applies a named control action ("start", "stop", "pause",
// "reload") to a channel: the one switch behind the JSON and HTML actions.
func (e *Engine) Lifecycle(ctx context.Context, id, action string) error {
	switch action {
	case "start":
		return e.Start(ctx, id)
	case "stop":
		return e.Stop(id)
	case "pause":
		return e.Pause(id)
	case "reload":
		return e.ReloadChannel(ctx, id)
	}
	return fmt.Errorf("%w %q", ErrUnknownAction, action)
}

// ScriptRef describes one script a channel references.
type ScriptRef struct {
	// Role is "filter", "transformer", "destination-filter:<id>", or
	// "destination-transformer:<id>".
	Role string `json:"role"`
	// Path is the resolved file path, usable with the scripts API.
	Path string `json:"path"`
	// LastError is the most recent compile error ("" when healthy).
	LastError string `json:"lastError,omitempty"`
}

// ScriptRefs lists a channel's scripts with their compile state.
func (e *Engine) ScriptRefs(id string) ([]ScriptRef, error) {
	cfg, ok := e.Config(id)
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownChannel, id)
	}
	var compileErrors map[string]string
	if e.scripts != nil {
		compileErrors = e.scripts.CompileErrors()
	}
	refs := []ScriptRef{}
	add := func(role, p string) {
		if p == "" {
			return
		}
		resolved := cfg.ResolvePath(p)
		refs = append(refs, ScriptRef{Role: role, Path: resolved, LastError: compileErrors[resolved]})
	}
	add("filter", cfg.Filter)
	for _, t := range cfg.Transformers {
		add("transformer", t)
	}
	for _, d := range cfg.Destinations {
		add("destination-filter:"+d.ID, d.Filter)
		for _, t := range d.Transformers {
			add("destination-transformer:"+d.ID, t)
		}
	}
	return refs, nil
}

// Stages lists the inspectable representations of a stored message in
// display order: received, transformed when a payload was stored, and one
// per destination.
func Stages(detail *store.MessageDetail) []string {
	stages := []string{StageReceived}
	if len(detail.Transformed) > 0 {
		stages = append(stages, StageTransformed)
	}
	for _, d := range detail.Destinations {
		stages = append(stages, "dest:"+d.DestinationID)
	}
	return stages
}
