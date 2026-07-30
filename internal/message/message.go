// Package message defines the canonical Message and its generic tree
// representation, shared by every format module, the pipeline, the script
// engine, and both UIs. Terminology follows Hohpe & Woolf's Enterprise
// Integration Patterns: a Message flows through Message Channels between
// filters and is transformed by Message Translators.
package message

import "time"

// Node is the generic message tree. Format modules parse raw bytes into it
// and serialize it back; transformer scripts operate on it through a format's
// path dialect. Repetition is expressed by sibling order. A Node is either a
// leaf (Value set, no Children) or an interior node (Children set); format
// modules maintain that invariant.
type Node struct {
	Name     string  `json:"name"`
	Value    string  `json:"value,omitempty"`
	Children []*Node `json:"children,omitempty"`
}

// IsLeaf reports whether the node carries a value directly.
func (n *Node) IsLeaf() bool { return len(n.Children) == 0 }

// Clone returns a deep copy of the subtree rooted at n.
func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	c := &Node{Name: n.Name, Value: n.Value}
	if len(n.Children) > 0 {
		c.Children = make([]*Node, len(n.Children))
		for i, ch := range n.Children {
			c.Children[i] = ch.Clone()
		}
	}
	return c
}

// State is the lifecycle state of a message (pipeline-level) or of a
// message/destination pair (delivery-level).
type State string

const (
	StateReceived    State = "RECEIVED"
	StateFiltered    State = "FILTERED" // dropped by a Message Filter; terminal but retained
	StateTransformed State = "TRANSFORMED"
	StateQueued      State = "QUEUED" // per destination
	StateSent        State = "SENT"   // per destination
	StateError       State = "ERROR"  // pipeline or per destination
)

// Message is the unit of work flowing through a channel.
type Message struct {
	ID            int64             // store-assigned; 0 until persisted
	ChannelID     string            //
	CorrelationID string            // stable across replays
	ReplayOf      int64             // original message ID when this is a replay; 0 otherwise
	Raw           []byte            // exact inbound bytes
	Tree          *Node             // parsed canonical tree (nil until parsed)
	DataType      string            // format module name, e.g. "hl7v2"
	State         State             //
	Error         string            // pipeline error detail when State == ERROR
	ReceivedAt    time.Time         //
	Meta          map[string]string // source context: filename, remote address, ...
}
