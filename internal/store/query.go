package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/langhorst/waggle/internal/message"
	metakey "github.com/langhorst/waggle/internal/meta"
)

// MessageSummary is one row of the message list.
type MessageSummary struct {
	ID            int64         `json:"id"`
	ChannelID     string        `json:"channelId"`
	CorrelationID string        `json:"correlationId"`
	ReplayOf      int64         `json:"replayOf,omitempty"`
	State         message.State `json:"state"`
	DataType      string        `json:"dataType"`
	ErrorText     string        `json:"errorText,omitempty"`
	ReceivedAt    time.Time     `json:"receivedAt"`
	UpdatedAt     time.Time     `json:"updatedAt"`
}

// DestinationStatus is one destination's outcome for a message.
type DestinationStatus struct {
	DestinationID string        `json:"destinationId"`
	State         message.State `json:"state"`
	Attempts      int           `json:"attempts"`
	LastError     string        `json:"lastError,omitempty"`
	DeadLetter    bool          `json:"deadLetter"`
	QueuedAt      *time.Time    `json:"queuedAt,omitempty"`
	SentAt        *time.Time    `json:"sentAt,omitempty"`
}

// MessageDetail is the full stored record of one message.
type MessageDetail struct {
	MessageSummary
	Raw                 []byte              `json:"-"`
	Transformed         []byte              `json:"-"`
	TransformedDataType string              `json:"transformedDataType,omitempty"`
	Meta                map[string]string   `json:"meta,omitempty"`
	Destinations        []DestinationStatus `json:"destinations"`
}

// ListQuery filters ListMessages. Zero values mean "no filter"; Limit
// defaults to 50.
type ListQuery struct {
	Limit    int
	BeforeID int64         // paging: only messages with ID < BeforeID
	State    message.State // filter by pipeline state
}

// ListMessages returns a channel's messages newest-first.
func (s *Store) ListMessages(ctx context.Context, channelID string, q ListQuery) ([]MessageSummary, error) {
	if q.Limit <= 0 {
		q.Limit = 50
	}
	query := `SELECT id, channel_id, correlation_id, COALESCE(replay_of, 0), state, data_type, error_text, received_at, updated_at
		FROM messages WHERE channel_id = ?`
	args := []any{channelID}
	if q.BeforeID > 0 {
		query += ` AND id < ?`
		args = append(args, q.BeforeID)
	}
	if q.State != "" {
		query += ` AND state = ?`
		args = append(args, string(q.State))
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, q.Limit)

	rows, err := s.reads.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list messages: %w", err)
	}
	defer rows.Close()
	var out []MessageSummary
	for rows.Next() {
		m, err := scanSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

type rowScanner interface{ Scan(dest ...any) error }

func scanSummary(r rowScanner) (MessageSummary, error) {
	var m MessageSummary
	var received, updated int64
	if err := r.Scan(&m.ID, &m.ChannelID, &m.CorrelationID, &m.ReplayOf, &m.State,
		&m.DataType, &m.ErrorText, &received, &updated); err != nil {
		return m, fmt.Errorf("store: scan message: %w", err)
	}
	m.ReceivedAt = time.UnixMilli(received)
	m.UpdatedAt = time.UnixMilli(updated)
	return m, nil
}

// ErrNotFound reports a missing message.
var ErrNotFound = errors.New("store: message not found")

// GetMessage loads one message with raw/transformed payloads and all
// destination outcomes.
func (s *Store) GetMessage(ctx context.Context, id int64) (*MessageDetail, error) {
	row := s.reads.QueryRowContext(ctx, `
		SELECT id, channel_id, correlation_id, COALESCE(replay_of, 0), state, data_type, error_text, received_at, updated_at,
		       raw, COALESCE(transformed, x''), transformed_data_type, meta_json
		FROM messages WHERE id = ?`, id)
	var d MessageDetail
	var received, updated int64
	var metaJSON string
	if err := row.Scan(&d.ID, &d.ChannelID, &d.CorrelationID, &d.ReplayOf, &d.State,
		&d.DataType, &d.ErrorText, &received, &updated, &d.Raw, &d.Transformed, &d.TransformedDataType, &metaJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("store: get message: %w", err)
	}
	d.ReceivedAt = time.UnixMilli(received)
	d.UpdatedAt = time.UnixMilli(updated)
	_ = json.Unmarshal([]byte(metaJSON), &d.Meta)

	rows, err := s.reads.QueryContext(ctx, `
		SELECT destination_id, state, attempts, last_error, dead_letter, queued_at, sent_at
		FROM message_destinations WHERE message_id = ? ORDER BY destination_id`, id)
	if err != nil {
		return nil, fmt.Errorf("store: get message destinations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ds DestinationStatus
		var deadLetter int
		var queuedAt, sentAt sql.NullInt64
		if err := rows.Scan(&ds.DestinationID, &ds.State, &ds.Attempts, &ds.LastError,
			&deadLetter, &queuedAt, &sentAt); err != nil {
			return nil, err
		}
		ds.DeadLetter = deadLetter == 1
		if queuedAt.Valid {
			t := time.UnixMilli(queuedAt.Int64)
			ds.QueuedAt = &t
		}
		if sentAt.Valid {
			t := time.UnixMilli(sentAt.Int64)
			ds.SentAt = &t
		}
		d.Destinations = append(d.Destinations, ds)
	}
	return &d, rows.Err()
}

// DestinationPayload returns the serialized bytes stored for one
// message/destination pair.
func (s *Store) DestinationPayload(ctx context.Context, id int64, destID string) ([]byte, error) {
	var payload []byte
	err := s.reads.QueryRowContext(ctx,
		`SELECT COALESCE(payload, x'') FROM message_destinations WHERE message_id = ? AND destination_id = ?`,
		id, destID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: destination payload: %w", err)
	}
	return payload, nil
}

// DLQEntry is one dead-lettered delivery, joined with its message summary.
type DLQEntry struct {
	MessageSummary
	Destination DestinationStatus `json:"destination"`
}

// DeadLetters lists a channel's dead-lettered deliveries plus its
// pipeline-level errors (the Invalid Message Channel view). Newest first.
func (s *Store) DeadLetters(ctx context.Context, channelID string, limit int) ([]DLQEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.reads.QueryContext(ctx, `
		SELECT m.id, m.channel_id, m.correlation_id, COALESCE(m.replay_of, 0), m.state, m.data_type, m.error_text, m.received_at, m.updated_at,
		       d.destination_id, d.state, d.attempts, d.last_error
		FROM message_destinations d
		JOIN messages m ON m.id = d.message_id
		WHERE m.channel_id = ? AND d.dead_letter = 1
		ORDER BY m.id DESC LIMIT ?`, channelID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: dead letters: %w", err)
	}
	defer rows.Close()
	var out []DLQEntry
	for rows.Next() {
		var e DLQEntry
		var received, updated int64
		if err := rows.Scan(&e.ID, &e.ChannelID, &e.CorrelationID, &e.ReplayOf, &e.State,
			&e.DataType, &e.ErrorText, &received, &updated,
			&e.Destination.DestinationID, &e.Destination.State, &e.Destination.Attempts, &e.Destination.LastError); err != nil {
			return nil, err
		}
		e.ReceivedAt = time.UnixMilli(received)
		e.UpdatedAt = time.UnixMilli(updated)
		e.Destination.DeadLetter = true
		out = append(out, e)
	}
	return out, rows.Err()
}

// MessageCounts returns per-state counts for a channel. Pipeline-level
// states come from the messages table; SENT and ERROR additionally include
// per-destination outcomes (a message's pipeline state stops at TRANSFORMED
// — delivery success and dead-lettering are recorded per destination).
func (s *Store) MessageCounts(ctx context.Context, channelID string) (map[message.State]int, error) {
	rows, err := s.reads.QueryContext(ctx,
		`SELECT state, COUNT(*) FROM messages WHERE channel_id = ? GROUP BY state`, channelID)
	if err != nil {
		return nil, fmt.Errorf("store: counts: %w", err)
	}
	defer rows.Close()
	out := map[message.State]int{}
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[message.State(state)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	destRows, err := s.reads.QueryContext(ctx, `
		SELECT d.state, COUNT(*)
		FROM message_destinations d
		JOIN messages m ON m.id = d.message_id
		WHERE m.channel_id = ? AND d.state IN ('SENT', 'ERROR')
		GROUP BY d.state`, channelID)
	if err != nil {
		return nil, fmt.Errorf("store: destination counts: %w", err)
	}
	defer destRows.Close()
	for destRows.Next() {
		var state string
		var n int
		if err := destRows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[message.State(state)] += n
	}
	return out, destRows.Err()
}

// NewReplay clones a stored message into a fresh pipeline-ready Message:
// same raw bytes and correlation ID, replay_of pointing at the original.
// The caller injects it into the channel pipeline.
func (s *Store) NewReplay(ctx context.Context, originalID int64) (*message.Message, error) {
	orig, err := s.GetMessage(ctx, originalID)
	if err != nil {
		return nil, err
	}
	return &message.Message{
		ChannelID:     orig.ChannelID,
		CorrelationID: orig.CorrelationID,
		ReplayOf:      originalID,
		Raw:           orig.Raw,
		DataType:      orig.DataType,
		State:         message.StateReceived,
		ReceivedAt:    time.Now(),
		Meta:          map[string]string{metakey.ReplayOf: fmt.Sprintf("%d", originalID)},
	}, nil
}
