package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/langhorst/integration-channel/internal/format"
	"github.com/langhorst/integration-channel/internal/message"
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
