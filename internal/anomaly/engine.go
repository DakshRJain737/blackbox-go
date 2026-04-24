// Package anomaly watches the pub/sub bus for threshold violations
// and fires anomaly events with rate-limiting.
package anomaly
 
import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
 
	"blackbox/internal/pubsub"
	"blackbox/internal/storage"
)
 
// ThresholdConfig defines when to fire an anomaly for a topic.
type ThresholdConfig struct {
	Above    *float64 `json:"above"`    // fire if value > above
	Below    *float64 `json:"below"`    // fire if value < below
	ValueKey string   `json:"value_key"` // which field in payload holds the value (default: "value")
}
 
// Engine watches the bus and fires anomaly events.
type Engine struct {
	mu             sync.Mutex
	thresholds     map[string]ThresholdConfig // topic → config
	thresholdsPath string
	bus            *pubsub.Bus
	db             *storage.DB
	lastFired      map[string]time.Time // topic → last anomaly time (for rate limiting)
	cooldown       time.Duration
}
 
// New creates an anomaly engine.
func New(thresholdsPath string, bus *pubsub.Bus, db *storage.DB) *Engine {
	e := &Engine{
		thresholds:     make(map[string]ThresholdConfig),
		thresholdsPath: thresholdsPath,
		bus:            bus,
		db:             db,
		lastFired:      make(map[string]time.Time),
		cooldown:       5 * time.Second,
	}
	if err := e.loadThresholds(); err != nil {
		log.Printf("⚠️  Anomaly thresholds not loaded: %v", err)
	}
	return e
}
 
func (e *Engine) loadThresholds() error {
	f, err := os.Open(e.thresholdsPath)
	if err != nil {
		return fmt.Errorf("open thresholds: %w", err)
	}
	defer f.Close()
 
	var t map[string]ThresholdConfig
	if err := json.NewDecoder(f).Decode(&t); err != nil {
		return fmt.Errorf("decode thresholds: %w", err)
	}
 
	e.mu.Lock()
	e.thresholds = t
	e.mu.Unlock()
 
	log.Printf("✅ Loaded anomaly thresholds for %d topics", len(t))
	return nil
}
 
// Run subscribes to all topics and processes each message.
func (e *Engine) Run(ctx context.Context) {
	ch, unsub := e.bus.SubscribeAll(2048)
	defer unsub()
 
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			if msg.Type != "MESSAGE" {
				continue
			}
			e.evaluate(msg)
		}
	}
}
 
func (e *Engine) evaluate(msg pubsub.Message) {
	e.mu.Lock()
	cfg, ok := e.thresholds[msg.Topic]
	e.mu.Unlock()
	if !ok {
		return
	}
 
	// Extract numeric value
	valueKey := cfg.ValueKey
	if valueKey == "" {
		valueKey = "value"
	}
	raw, ok := msg.Payload[valueKey]
	if !ok {
		return
	}
	val, ok := toFloat64(raw)
	if !ok {
		return
	}
 
	// Check thresholds
	var direction string
	var threshold float64
 
	if cfg.Above != nil && val > *cfg.Above {
		direction = "above"
		threshold = *cfg.Above
	} else if cfg.Below != nil && val < *cfg.Below {
		direction = "below"
		threshold = *cfg.Below
	} else {
		return
	}
 
	// Rate limit: don't fire same topic anomaly more than once per cooldown
	e.mu.Lock()
	last, seen := e.lastFired[msg.Topic]
	if seen && time.Since(last) < e.cooldown {
		e.mu.Unlock()
		return
	}
	e.lastFired[msg.Topic] = time.Now()
	e.mu.Unlock()
 
	// Log to DB
	if err := e.db.InsertAnomaly(msg.Topic, msg.Device, val, threshold, direction, msg.Timestamp); err != nil {
		log.Printf("anomaly DB write error: %v", err)
	}
 
	// Build anomaly message
	anomMsg := pubsub.Message{
		Topic:  "/anomaly",
		Device: msg.Device,
		Payload: map[string]any{
			"source_topic": msg.Topic,
			"value":        val,
			"threshold":    threshold,
			"direction":    direction,
			"message":      fmt.Sprintf("%s value %.2f is %s threshold %.2f", msg.Topic, val, direction, threshold),
		},
		Timestamp: msg.Timestamp,
		Type:      "ANOMALY",
	}
 
	log.Printf("🚨 ANOMALY on %s: value=%.2f %s threshold=%.2f", msg.Topic, val, direction, threshold)
	e.bus.Publish(anomMsg)
}
 
// Thresholds returns a copy of current threshold config (for the API).
func (e *Engine) Thresholds() map[string]ThresholdConfig {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]ThresholdConfig, len(e.thresholds))
	for k, v := range e.thresholds {
		out[k] = v
	}
	return out
}
 
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}
 