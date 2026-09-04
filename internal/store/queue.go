package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/langhorst/waggle/internal/message"
)

// QueueItem is the head of one destination's delivery queue.
type QueueItem struct {
	QueueID   int64
	MessageID int64
	Payload   []byte
	// Meta is the delivery's stored metadata (script meta writes that
	// outbound adapters read); nil when none was recorded.
	Meta      map[string]string
	Attempts  int
	NotBefore time.Time
	// Orphaned marks a queue row with no recorded payload behind it (the
	// destination record is missing or its payload is NULL). There is
	// nothing to send; the worker dead-letters it.
	Orphaned bool
}

// Enqueue adds a pending delivery for (channel, destination, message). The
// destination row (state QUEUED, payload) must already be recorded via
// SetDestinationState — the pipeline does both in order.
func (s *Store) Enqueue(ctx context.Context, channelID, destID string, messageID int64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO destination_queue (channel_id, destination_id, message_id) VALUES (?, ?, ?)`,
		channelID, destID, messageID)
	if err != nil {
		return fmt.Errorf("store: enqueue: %w", err)
	}
	s.signal(channelID, destID)
	return nil
}

// Head returns the oldest queued delivery for a destination (FIFO — the
// caller must respect NotBefore), or nil when the queue is empty.
func (s *Store) Head(ctx context.Context, channelID, destID string) (*QueueItem, error) {
	row := s.reads.QueryRowContext(ctx, `
		SELECT q.id, q.message_id, q.not_before, COALESCE(d.payload, x''), d.payload IS NULL, COALESCE(d.meta, ''), COALESCE(d.attempts, 0)
		FROM destination_queue q
		LEFT JOIN message_destinations d ON d.message_id = q.message_id AND d.destination_id = q.destination_id
		WHERE q.channel_id = ? AND q.destination_id = ?
		ORDER BY q.id LIMIT 1`, channelID, destID)
	var item QueueItem
	var notBefore int64
	var metaJSON string
	if err := row.Scan(&item.QueueID, &item.MessageID, &notBefore, &item.Payload, &item.Orphaned, &metaJSON, &item.Attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: queue head: %w", err)
	}
	if metaJSON != "" {
		if err := json.Unmarshal([]byte(metaJSON), &item.Meta); err != nil {
			return nil, fmt.Errorf("store: queue head: decoding meta: %w", err)
		}
	}
	item.NotBefore = time.UnixMilli(notBefore)
	return &item, nil
}

// MarkSent completes a delivery: destination state SENT and queue row
// removed in one transaction, so a crash can duplicate a send (at-least-
// once) but never lose one.
func (s *Store) MarkSent(ctx context.Context, item *QueueItem, destID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: mark sent: %w", err)
	}
	now := nowMillis()
	if _, err := tx.ExecContext(ctx, `
		UPDATE message_destinations SET state = ?, last_error = '', sent_at = ?, updated_at = ?
		WHERE message_id = ? AND destination_id = ?`,
		string(message.StateSent), now, now, item.MessageID, destID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: mark sent: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM destination_queue WHERE id = ?`, item.QueueID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: mark sent: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: mark sent: %w", err)
	}
	return nil
}

// Backoff records a failed attempt and schedules the retry.
func (s *Store) Backoff(ctx context.Context, item *QueueItem, destID string, notBefore time.Time, errText string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: backoff: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE message_destinations SET attempts = attempts + 1, last_error = ?, updated_at = ?
		WHERE message_id = ? AND destination_id = ?`,
		errText, nowMillis(), item.MessageID, destID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: backoff: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE destination_queue SET not_before = ? WHERE id = ?`,
		notBefore.UnixMilli(), item.QueueID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: backoff: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: backoff: %w", err)
	}
	return nil
}

// DeadLetter abandons a delivery: destination state ERROR (dead_letter=1)
// and the queue row removed, in one transaction. The message stays stored
// and can be requeued from the DLQ view.
func (s *Store) DeadLetter(ctx context.Context, item *QueueItem, destID string, errText string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: dead letter: %w", err)
	}
	now := nowMillis()
	if _, err := tx.ExecContext(ctx, `
		UPDATE message_destinations SET state = ?, attempts = attempts + 1, last_error = ?, dead_letter = 1, updated_at = ?
		WHERE message_id = ? AND destination_id = ?`,
		string(message.StateError), errText, now, item.MessageID, destID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: dead letter: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM destination_queue WHERE id = ?`, item.QueueID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: dead letter: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: dead letter: %w", err)
	}
	return nil
}

// Requeue puts a dead-lettered (or previously sent) delivery back on the
// queue with a fresh attempt budget, reusing the stored payload.
func (s *Store) Requeue(ctx context.Context, messageID int64, destID string) error {
	var channelID string
	err := s.db.QueryRowContext(ctx,
		`SELECT m.channel_id FROM messages m
		 JOIN message_destinations d ON d.message_id = m.id
		 WHERE m.id = ? AND d.destination_id = ?`, messageID, destID).Scan(&channelID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("store: requeue: no destination %q for message %d", destID, messageID)
	}
	if err != nil {
		return fmt.Errorf("store: requeue: %w", err)
	}
	var pending int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM destination_queue WHERE message_id = ? AND destination_id = ?`,
		messageID, destID).Scan(&pending); err != nil {
		return fmt.Errorf("store: requeue: %w", err)
	}
	if pending > 0 {
		return fmt.Errorf("store: requeue: message %d is already queued for %s", messageID, destID)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: requeue: %w", err)
	}
	now := nowMillis()
	if _, err := tx.ExecContext(ctx, `
		UPDATE message_destinations SET state = ?, attempts = 0, last_error = '', dead_letter = 0, queued_at = ?, updated_at = ?
		WHERE message_id = ? AND destination_id = ?`,
		string(message.StateQueued), now, now, messageID, destID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: requeue: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO destination_queue (channel_id, destination_id, message_id) VALUES (?, ?, ?)`,
		channelID, destID, messageID); err != nil {
		tx.Rollback()
		return fmt.Errorf("store: requeue: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: requeue: %w", err)
	}
	s.signal(channelID, destID)
	return nil
}

// QueueDepth reports pending deliveries per destination for a channel.
func (s *Store) QueueDepth(ctx context.Context, channelID string) (map[string]int, error) {
	rows, err := s.reads.QueryContext(ctx,
		`SELECT destination_id, COUNT(*) FROM destination_queue WHERE channel_id = ? GROUP BY destination_id`,
		channelID)
	if err != nil {
		return nil, fmt.Errorf("store: queue depth: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var dest string
		var n int
		if err := rows.Scan(&dest, &n); err != nil {
			return nil, err
		}
		out[dest] = n
	}
	return out, rows.Err()
}
