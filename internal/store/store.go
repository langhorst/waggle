// Package store is the SQLite persistence layer: it implements the
// pipeline's Recorder seam, the Guaranteed Delivery queue, message history
// queries for the UIs, count-based retention, and replay. Pure Go via
// modernc.org/sqlite — no CGo, nothing to install.
package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"

	"github.com/langhorst/waggle/internal/message"
)

//go:embed migrations/*.sql
var migrations embed.FS

// pruneCheckEvery is how many message inserts may elapse between retention
// sweeps (a time-based ticker also runs; see Open).
const pruneCheckEvery = 100

// Store wraps the SQLite database. Safe for concurrent use; writes are
// serialized on a single connection (WAL mode).
type Store struct {
	db *sql.DB

	retMu     sync.Mutex
	retention map[string]int // channel -> max messages; -1 unlimited

	inserts     atomic.Int64
	pruneReq    chan struct{} // nudges pruneLoop; never blocks Record
	pruneCancel context.CancelFunc
	pruneDone   chan struct{}
}

// Open opens (creating if needed) the database at path and applies pending
// migrations.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)", filepath.ToSlash(path))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	// One connection serializes all access: modernc/sqlite performs best
	// this way and it sidesteps SQLITE_BUSY between our own goroutines.
	db.SetMaxOpenConns(1)

	s := &Store{db: db, retention: map[string]int{}, pruneReq: make(chan struct{}, 1)}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}

	pruneCtx, cancel := context.WithCancel(context.Background())
	s.pruneCancel = cancel
	s.pruneDone = make(chan struct{})
	go s.pruneLoop(pruneCtx)
	return s, nil
}

// Close stops background work and closes the database.
func (s *Store) Close() error {
	if s.pruneCancel != nil {
		s.pruneCancel()
		<-s.pruneDone
	}
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY)`); err != nil {
		return fmt.Errorf("store: migrations table: %w", err)
	}
	entries, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(entries)
	for _, name := range entries {
		var applied int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE name = ?`, name).Scan(&applied); err != nil {
			return fmt.Errorf("store: %w", err)
		}
		if applied > 0 {
			continue
		}
		raw, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(raw)); err != nil {
			tx.Rollback()
			return fmt.Errorf("store: applying %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (name) VALUES (?)`, name); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func nowMillis() int64 { return time.Now().UnixMilli() }

// SetRetention configures the per-channel message cap (-1 = unlimited).
func (s *Store) SetRetention(channelID string, count int) {
	s.retMu.Lock()
	s.retention[channelID] = count
	s.retMu.Unlock()
}

// ---- channel.Recorder implementation ----

// Record inserts a freshly received message and assigns its ID.
func (s *Store) Record(ctx context.Context, m *message.Message) error {
	metaJSON, err := json.Marshal(m.Meta)
	if err != nil {
		metaJSON = []byte("{}")
	}
	var replayOf any
	if m.ReplayOf != 0 {
		replayOf = m.ReplayOf
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO messages (channel_id, correlation_id, replay_of, state, data_type, raw, meta_json, received_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ChannelID, m.CorrelationID, replayOf, string(message.StateReceived),
		m.DataType, m.Raw, string(metaJSON), m.ReceivedAt.UnixMilli(), nowMillis())
	if err != nil {
		return fmt.Errorf("store: record: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("store: record: %w", err)
	}
	m.ID = id

	if s.inserts.Add(1)%pruneCheckEvery == 0 {
		// Retention runs on its own goroutine: a DELETE with subselects
		// does not belong on the pipeline's write path.
		select {
		case s.pruneReq <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *Store) SetState(ctx context.Context, id int64, state message.State, errText string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages SET state = ?, error_text = ?, updated_at = ? WHERE id = ?`,
		string(state), errText, nowMillis(), id)
	if err != nil {
		return fmt.Errorf("store: set state: %w", err)
	}
	return nil
}

func (s *Store) SetTransformed(ctx context.Context, id int64, payload []byte, dataType string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages SET transformed = ?, transformed_data_type = ?, updated_at = ? WHERE id = ?`,
		payload, dataType, nowMillis(), id)
	if err != nil {
		return fmt.Errorf("store: set transformed: %w", err)
	}
	return nil
}

// SetDestinationState upserts one destination's outcome. A nil payload
// (or nil meta) preserves any previously stored value. state=ERROR marks the
// row dead-lettered (retrying deliveries stay QUEUED until they exhaust).
func (s *Store) SetDestinationState(ctx context.Context, id int64, destID string, state message.State, payload []byte, meta map[string]string, errText string) error {
	now := nowMillis()
	deadLetter := 0
	if state == message.StateError {
		deadLetter = 1
	}
	var queuedAt, sentAt any
	if state == message.StateQueued {
		queuedAt = now
	}
	if state == message.StateSent {
		sentAt = now
	}
	var metaJSON any
	if meta != nil {
		encoded, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("store: set destination state: encoding meta: %w", err)
		}
		metaJSON = string(encoded)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO message_destinations (message_id, destination_id, state, payload, meta, last_error, dead_letter, queued_at, sent_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (message_id, destination_id) DO UPDATE SET
			state = excluded.state,
			payload = COALESCE(excluded.payload, message_destinations.payload),
			meta = COALESCE(excluded.meta, message_destinations.meta),
			last_error = excluded.last_error,
			dead_letter = excluded.dead_letter,
			queued_at = COALESCE(excluded.queued_at, message_destinations.queued_at),
			sent_at = COALESCE(excluded.sent_at, message_destinations.sent_at),
			updated_at = excluded.updated_at`,
		id, destID, string(state), payload, metaJSON, errText, deadLetter, queuedAt, sentAt, now)
	if err != nil {
		return fmt.Errorf("store: set destination state: %w", err)
	}
	return nil
}

// ---- retention ----

func (s *Store) pruneLoop(ctx context.Context) {
	defer close(s.pruneDone)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.pruneReq:
		}
		s.pruneAll(ctx)
	}
}

func (s *Store) pruneAll(ctx context.Context) {
	s.retMu.Lock()
	snapshot := make(map[string]int, len(s.retention))
	for k, v := range s.retention {
		snapshot[k] = v
	}
	s.retMu.Unlock()

	for channelID, keep := range snapshot {
		if keep < 0 {
			continue
		}
		// Keep the newest `keep` messages; never delete a message still
		// referenced by the delivery queue.
		_, _ = s.db.ExecContext(ctx, `
			DELETE FROM messages
			WHERE channel_id = ?1
			  AND id NOT IN (SELECT id FROM messages WHERE channel_id = ?1 ORDER BY id DESC LIMIT ?2)
			  AND id NOT IN (SELECT message_id FROM destination_queue WHERE channel_id = ?1)`,
			channelID, keep)
	}
}

// Prune runs one immediate retention sweep (mainly for tests).
func (s *Store) Prune(ctx context.Context) { s.pruneAll(ctx) }
