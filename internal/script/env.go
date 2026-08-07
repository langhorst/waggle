package script

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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
	metaMap  map[string]any

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

	// goja wraps Go maps by reference, so script writes land in metaMap;
	// syncMeta folds them back into the message after a successful run.
	e.metaMap = make(map[string]any, len(m.Meta))
	for k, v := range m.Meta {
		e.metaMap[k] = v
	}
	_ = rt.Set("meta", e.metaMap)
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
			return setValue(sm.dt, sm.tree(), path, value)
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
	join := func(segName, rel string) string { return segName + "-" + rel }
	if j, ok := sm.dt.(format.SegmentJoiner); ok {
		join = j.JoinSegmentPath
	}
	return map[string]any{
		"name": seg.Name,
		"get": func(rel string) (any, error) {
			nodes, err := sm.dt.Resolve(scoped, join(seg.Name, rel))
			if err != nil || len(nodes) == 0 {
				return nil, err
			}
			return sm.dt.Value(sm.tree(), nodes[0]), nil
		},
		"set": func(rel string, value goja.Value) error {
			return setValue(sm.dt, scoped, join(seg.Name, rel), value)
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

// syncMeta folds script writes to the meta object back into the message:
// scalars are stringified, deleted keys disappear, and anything
// non-scalar (functions, objects) is dropped. Called only after a
// successful run so a failed script leaves meta untouched.
func (e *env) syncMeta() {
	meta := make(map[string]string, len(e.metaMap))
	for k, v := range e.metaMap {
		switch t := v.(type) {
		case string:
			meta[k] = t
		case bool:
			meta[k] = strconv.FormatBool(t)
		case int64:
			meta[k] = strconv.FormatInt(t, 10)
		case float64:
			meta[k] = strconv.FormatFloat(t, 'g', -1, 64)
		}
	}
	e.m.Meta = meta
}

// setValue writes a script-provided value at path. Formats that distinguish
// value types (format.TypedSetter — JSON) receive the native JS type so
// numbers and booleans stay typed on the wire; everything else gets the JS
// string conversion.
func setValue(dt format.DataType, root *message.Node, path string, v goja.Value) error {
	if ts, ok := dt.(format.TypedSetter); ok {
		return ts.SetTyped(root, path, jsScalar(v))
	}
	return dt.Set(root, path, jsString(v))
}

// jsScalar exports a JS value as a Go scalar for a TypedSetter:
// null/undefined → nil, primitives keep their type, anything else falls back
// to JS string conversion.
func jsScalar(v goja.Value) any {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return nil
	}
	switch t := v.Export().(type) {
	case string, bool, int64, float64:
		return t
	default:
		return v.String()
	}
}

// jsString renders a JS value for storage in the tree: null/undefined
// become empty, everything else uses JS string conversion.
func jsString(v goja.Value) string {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return ""
	}
	return v.String()
}
