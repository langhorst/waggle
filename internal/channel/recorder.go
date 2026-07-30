package channel

import (
	"context"
	"sync/atomic"

	"github.com/langhorst/integration-channel/internal/message"
)

// MemoryRecorder is the no-persistence Recorder: it assigns process-local
// message IDs and forgets everything else. Used until the SQLite store
// lands, and by tests that only care about pipeline behavior.
type MemoryRecorder struct {
	nextID atomic.Int64
}

func NewMemoryRecorder() *MemoryRecorder { return &MemoryRecorder{} }

func (r *MemoryRecorder) Record(ctx context.Context, m *message.Message) error {
	m.ID = r.nextID.Add(1)
	return nil
}

func (r *MemoryRecorder) SetState(ctx context.Context, id int64, state message.State, errText string) error {
	return nil
}

func (r *MemoryRecorder) SetTransformed(ctx context.Context, id int64, payload []byte) error {
	return nil
}

func (r *MemoryRecorder) SetDestinationState(ctx context.Context, id int64, destID string, state message.State, payload []byte, errText string) error {
	return nil
}
