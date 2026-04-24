// Package storage wraps SQLite for persistent message logging.
// WAL mode is enabled so reads never block writes.
package storage
 
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"
 
	_ "github.com/mattn/go-sqlite3"
 
	"blackbox/internal/pubsub"
)
 
// DB wraps an SQLite connection pool.
type DB struct {
	conn *sql.DB
}
 
// New opens (or creates) the SQLite database at path and runs migrations.
func New(path string) (*DB, error) {
	conn, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
 
	// Small pool — WAL handles concurrency
	conn.SetMaxOpenConns(5)
	conn.SetMaxIdleConns(3)
 
	db := &DB{conn: conn}
	if err := db.migrate(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}
 
// Close releases the database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}
 
// ---- Migrations ----
 
func (db *DB) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS messages (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			topic     TEXT    NOT NULL,
			device    TEXT    NOT NULL,
			payload   TEXT    NOT NULL,
			ts        INTEGER NOT NULL,  -- unix milliseconds
			type      TEXT    NOT NULL DEFAULT 'MESSAGE'
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_ts    ON messages(ts)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_device ON messages(device)`,
 
		`CREATE TABLE IF NOT EXISTS anomalies (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			topic     TEXT    NOT NULL,
			device    TEXT    NOT NULL,
			value     REAL    NOT NULL,
			threshold REAL    NOT NULL,
			direction TEXT    NOT NULL,  -- "above" | "below"
			ts        INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_anomalies_ts ON anomalies(ts)`,
 
		`CREATE TABLE IF NOT EXISTS schema_errors (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			device    TEXT    NOT NULL,
			raw       TEXT    NOT NULL,
			reason    TEXT    NOT NULL,
			ts        INTEGER NOT NULL
		)`,
 
		`CREATE TABLE IF NOT EXISTS sessions (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			started_at INTEGER NOT NULL,
			ended_at   INTEGER,
			notes      TEXT
		)`,
	}
 
	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
 
	for _, m := range migrations {
		if _, err := tx.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w\nSQL: %s", err, m)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Println("✅ Database migrations applied")
	return nil
}
 
// ---- Writes ----
 
// InsertMessage persists a sensor message.
func (db *DB) InsertMessage(msg pubsub.Message) error {
	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	_, err = db.conn.Exec(
		`INSERT INTO messages(topic, device, payload, ts, type) VALUES(?,?,?,?,?)`,
		msg.Topic, msg.Device, string(payload), msg.Timestamp.UnixMilli(), msg.Type,
	)
	return err
}
 
// InsertAnomaly persists an anomaly event.
func (db *DB) InsertAnomaly(topic, device string, value, threshold float64, direction string, ts time.Time) error {
	_, err := db.conn.Exec(
		`INSERT INTO anomalies(topic, device, value, threshold, direction, ts) VALUES(?,?,?,?,?,?)`,
		topic, device, value, threshold, direction, ts.UnixMilli(),
	)
	return err
}
 
// InsertSchemaError persists a schema validation failure.
func (db *DB) InsertSchemaError(msg pubsub.Message) error {
	raw, _ := msg.Payload["raw"].(string)
	reason, _ := msg.Payload["reason"].(string)
	_, err := db.conn.Exec(
		`INSERT INTO schema_errors(device, raw, reason, ts) VALUES(?,?,?,?)`,
		msg.Device, raw, reason, msg.Timestamp.UnixMilli(),
	)
	return err
}
 
// ---- Reads ----
 
// ReplayRow represents a single row from the messages table.
type ReplayRow struct {
	ID      int64
	Topic   string
	Device  string
	Payload map[string]any
	TS      time.Time
	Type    string
}
 
// QueryMessages returns messages in a time window, optionally filtered by topic.
// Supports pagination via limit/offset.
func (db *DB) QueryMessages(topic string, from, to time.Time, limit, offset int) ([]ReplayRow, error) {
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
 
	var (
		rows *sql.Rows
		err  error
	)
 
	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()
 
	if topic == "" {
		rows, err = db.conn.Query(
			`SELECT id, topic, device, payload, ts, type
			 FROM messages
			 WHERE ts >= ? AND ts <= ?
			 ORDER BY ts ASC
			 LIMIT ? OFFSET ?`,
			fromMs, toMs, limit, offset,
		)
	} else {
		rows, err = db.conn.Query(
			`SELECT id, topic, device, payload, ts, type
			 FROM messages
			 WHERE topic = ? AND ts >= ? AND ts <= ?
			 ORDER BY ts ASC
			 LIMIT ? OFFSET ?`,
			topic, fromMs, toMs, limit, offset,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()
 
	return scanRows(rows)
}
 
func scanRows(rows *sql.Rows) ([]ReplayRow, error) {
	var results []ReplayRow
	for rows.Next() {
		var r ReplayRow
		var payloadStr string
		var tsMs int64
		if err := rows.Scan(&r.ID, &r.Topic, &r.Device, &payloadStr, &tsMs, &r.Type); err != nil {
			return nil, err
		}
		r.TS = time.UnixMilli(tsMs).UTC()
		if err := json.Unmarshal([]byte(payloadStr), &r.Payload); err != nil {
			r.Payload = map[string]any{"raw": payloadStr}
		}
		results = append(results, r)
	}
	return results, rows.Err()
}
 
// Stats returns aggregate counts useful for the dashboard.
func (db *DB) Stats() (map[string]any, error) {
	stats := make(map[string]any)
 
	row := db.conn.QueryRow(`SELECT COUNT(*) FROM messages`)
	var total int64
	if err := row.Scan(&total); err != nil {
		return nil, err
	}
	stats["total_messages"] = total
 
	row = db.conn.QueryRow(`SELECT COUNT(*) FROM anomalies`)
	var anomCount int64
	if err := row.Scan(&anomCount); err != nil {
		return nil, err
	}
	stats["total_anomalies"] = anomCount
 
	row = db.conn.QueryRow(`SELECT COUNT(*) FROM schema_errors`)
	var schemaErrCount int64
	if err := row.Scan(&schemaErrCount); err != nil {
		return nil, err
	}
	stats["schema_errors"] = schemaErrCount
 
	// Topics and their message counts
	rows, err := db.conn.Query(`SELECT topic, COUNT(*) as cnt FROM messages GROUP BY topic ORDER BY cnt DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type topicStat struct {
		Topic string `json:"topic"`
		Count int64  `json:"count"`
	}
	var topicStats []topicStat
	for rows.Next() {
		var ts topicStat
		if err := rows.Scan(&ts.Topic, &ts.Count); err != nil {
			continue
		}
		topicStats = append(topicStats, ts)
	}
	stats["topics"] = topicStats
 
	return stats, nil
}
 
// RecentAnomalies returns the N most recent anomaly events.
func (db *DB) RecentAnomalies(n int) ([]map[string]any, error) {
	rows, err := db.conn.Query(
		`SELECT topic, device, value, threshold, direction, ts
		 FROM anomalies ORDER BY ts DESC LIMIT ?`, n,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
 
	var results []map[string]any
	for rows.Next() {
		var topic, device, direction string
		var value, threshold float64
		var tsMs int64
		if err := rows.Scan(&topic, &device, &value, &threshold, &direction, &tsMs); err != nil {
			continue
		}
		results = append(results, map[string]any{
			"topic":     topic,
			"device":    device,
			"value":     value,
			"threshold": threshold,
			"direction": direction,
			"ts":        time.UnixMilli(tsMs).UTC(),
		})
	}
	return results, rows.Err()
}