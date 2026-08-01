package script

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dop251/goja"

	"github.com/langhorst/waggle/internal/channel"
	"github.com/langhorst/waggle/internal/format"
	"github.com/langhorst/waggle/internal/message"
)

// env is the per-execution binding state: the msg wrapper, any messages the
// script created via newMessage, and the response outcome.
type env struct {
	rt       *goja.Runtime
	m        *message.Message
	msgValue goja.Value

	created   []*scriptMsg
	rejection *channel.Rejection
}

// scriptMsg is a message tree exposed to the script — either the live
// pipeline message or one created by newMessage for format conversion.
type scriptMsg struct {
	dt   format.DataType
	tree func() *message.Node
}

func newEnv(rt *goja.Runtime, m *message.Message) (*env, error) {
	dt, ok := format.Get(m.DataType)
	if !ok {
		return nil, fmt.Errorf("unknown data type %q", m.DataType)
	}
	e := &env{rt: rt, m: m}

	live := &scriptMsg{dt: dt, tree: func() *message.Node { return m.Tree }}
	msgObj := e.wrapMsg(live, 0)
	msgObj["id"] = m.ID
	msgObj["channel"] = m.ChannelID
	msgObj["raw"] = string(m.Raw)
	e.msgValue = rt.ToValue(msgObj)

	meta := make(map[string]any, len(m.Meta))
	for k, v := range m.Meta {
		meta[k] = v
	}
	_ = rt.Set("meta", meta)
	_ = rt.Set("msg", e.msgValue)
	_ = rt.Set("logger", e.loggerObj())
	_ = rt.Set("response", e.responseObj())
	_ = rt.Set("newMessage", e.newMessageFn())
	return e, nil
}

// wrapMsg builds the JS-facing object for one message tree. handle 0 is the
// live pipeline message; created messages get 1-based handles used by
// applyResult to recognize a returned conversion result.
func (e *env) wrapMsg(sm *scriptMsg, handle int) map[string]any {
	obj := map[string]any{
		"_handle":  handle,
		"dataType": sm.dt.Name(),
		"get": func(path string) (any, error) {
			nodes, err := sm.dt.Resolve(sm.tree(), path)
			if err != nil || len(nodes) == 0 {
				return nil, err
			}
			return sm.dt.Value(sm.tree(), nodes[0]), nil
		},
		"getAll": func(path string) ([]string, error) {
			nodes, err := sm.dt.Resolve(sm.tree(), path)
			if err != nil {
				return nil, err
			}
			vals := make([]string, len(nodes))
			for i, n := range nodes {
				vals[i] = sm.dt.Value(sm.tree(), n)
			}
			return vals, nil
		},
		"set": func(path string, value goja.Value) error {
			return sm.dt.Set(sm.tree(), path, jsString(value))
		},
		"segments": func(name string) []map[string]any {
			segs := sm.dt.Segments(sm.tree(), name)
			out := make([]map[string]any, len(segs))
			for i, seg := range segs {
				out[i] = e.wrapSegment(sm, seg)
			}
			return out
		},
	}
	return obj
}

// wrapSegment exposes one segment/record/row with segment-relative paths:
// seg.get('5.2') on a PID handle resolves PID-5.2 against just that
// occurrence.
func (e *env) wrapSegment(sm *scriptMsg, seg *message.Node) map[string]any {
	// A single-segment root lets the format dialect resolve relative paths
	// against exactly this occurrence; the shared node pointer means sets
	// mutate the real tree.
	scoped := &message.Node{Name: sm.tree().Name, Children: []*message.Node{seg}}
	return map[string]any{
		"name": seg.Name,
		"get": func(rel string) (any, error) {
			nodes, err := sm.dt.Resolve(scoped, seg.Name+"-"+rel)
			if err != nil || len(nodes) == 0 {
				return nil, err
			}
			return sm.dt.Value(sm.tree(), nodes[0]), nil
		},
		"set": func(rel string, value goja.Value) error {
			return sm.dt.Set(scoped, seg.Name+"-"+rel, jsString(value))
		},
		"value": func() string {
			return sm.dt.Value(sm.tree(), seg)
		},
	}
}

func (e *env) loggerObj() map[string]any {
	log := slog.Default().With("script.channel", e.m.ChannelID, "script.message", e.m.ID)
	join := func(args []goja.Value) string {
		parts := make([]string, len(args))
		for i, a := range args {
			parts[i] = a.String()
		}
		return strings.Join(parts, " ")
	}
	return map[string]any{
		"info":  func(args ...goja.Value) { log.Info(join(args)) },
		"warn":  func(args ...goja.Value) { log.Warn(join(args)) },
		"error": func(args ...goja.Value) { log.Error(join(args)) },
	}
}

func (e *env) responseObj() map[string]any {
	return map[string]any{
		// reject stops the script (by throwing) and routes the message to
		// the Invalid Message Channel with exactly this ACK.
		"reject": func(code string, text ...string) (any, error) {
			e.rejection = &channel.Rejection{Code: code, Text: strings.Join(text, " ")}
			return nil, errors.New(e.rejection.Error())
		},
		// setAck overrides the ACK for destination-ACK sources without
		// stopping processing.
		"setAck": func(code string, text ...string) {
			e.m.AckCode = code
			e.m.AckText = strings.Join(text, " ")
		},
	}
}

func (e *env) newMessageFn() func(dtName string) (map[string]any, error) {
	return func(dtName string) (map[string]any, error) {
		dt, ok := format.Get(dtName)
		if !ok {
			return nil, fmt.Errorf("newMessage: unknown data type %q (registered: %v)", dtName, format.Names())
		}
		root := &message.Node{Name: dt.Name()}
		sm := &scriptMsg{dt: dt, tree: func() *message.Node { return root }}
		e.created = append(e.created, sm)
		return e.wrapMsg(sm, len(e.created)), nil
	}
}

// applyResult handles a transformer's return value: returning a message
// built with newMessage replaces the pipeline message's tree and data type
// (format conversion); anything else means "mutated in place".
func (e *env) applyResult(v goja.Value, m *message.Message) error {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return nil
	}
	exported := v.Export()
	obj, ok := exported.(map[string]any)
	if !ok {
		return nil // filters return booleans; ignore other values
	}
	handle, ok := obj["_handle"].(int)
	if !ok || handle == 0 {
		return nil // the live message itself, or not a message at all
	}
	if handle < 1 || handle > len(e.created) {
		return fmt.Errorf("returned message is not from newMessage")
	}
	sm := e.created[handle-1]
	m.Tree = sm.tree()
	m.DataType = sm.dt.Name()
	return nil
}

// jsString renders a JS value for storage in the tree: null/undefined
// become empty, everything else uses JS string conversion.
func jsString(v goja.Value) string {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return ""
	}
	return v.String()
}
