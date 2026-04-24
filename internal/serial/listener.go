// Package serial handles reading raw bytes from serial-connected devices
// (e.g. Arduino) and converting them into pubsub.Messages.
//
// Expected Arduino output format — one JSON line per message:
//
//	{"topic":"/distance","value":42.5}
//	{"topic":"/tilt","value":12.3,"extra":"any-extra-field"}
//
// The listener handles reconnection automatically.
package serial
 
import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"
 
	goserial "go.bug.st/serial"
 
	"blackbox/internal/pubsub"
	"blackbox/internal/schema"
	"blackbox/internal/storage"
	"blackbox/internal/watchdog"
)
 
// DeviceConfig mirrors the top-level config shape for a device.
type DeviceConfig struct {
	Name     string
	Port     string
	BaudRate int
}
 
// rawMessage is what we expect the Arduino to send over serial.
type rawMessage struct {
	Topic string         `json:"topic"`
	Value float64        `json:"value"`
	Extra map[string]any `json:"-"` // captured via custom unmarshal
}
 
// Listener reads from one serial port and publishes to the bus.
type Listener struct {
	dev       DeviceConfig
	bus       *pubsub.Bus
	db        *storage.DB
	validator *schema.Validator
	wd        *watchdog.Watchdog
}
 
// NewListener creates a new serial listener for a device.
func NewListener(dev DeviceConfig, bus *pubsub.Bus, db *storage.DB, v *schema.Validator, wd *watchdog.Watchdog) *Listener {
	return &Listener{dev: dev, bus: bus, db: db, validator: v, wd: wd}
}
 
// Run opens the serial port and reads lines forever, reconnecting on error.
// Blocks until ctx is cancelled.
func (l *Listener) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
 
		log.Printf("[%s] Opening serial port %s at %d baud", l.dev.Name, l.dev.Port, l.dev.BaudRate)
		port, err := goserial.Open(l.dev.Port, &goserial.Mode{BaudRate: l.dev.BaudRate})
		if err != nil {
			log.Printf("[%s] Failed to open port: %v — retrying in 3s", l.dev.Name, err)
			// Broadcast NODE_DEATH on failed open
			l.bus.Publish(pubsub.Message{
				Topic:     "/node_death",
				Device:    l.dev.Name,
				Payload:   map[string]any{"reason": err.Error()},
				Timestamp: time.Now().UTC(),
				Type:      "NODE_DEATH",
			})
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}
 
		log.Printf("[%s] ✅ Connected on %s", l.dev.Name, l.dev.Port)
 
		// Notify watchdog that this node is alive
		l.wd.Heartbeat(l.dev.Name)
 
		// Broadcast NODE_ALIVE
		l.bus.Publish(pubsub.Message{
			Topic:     "/node_alive",
			Device:    l.dev.Name,
			Payload:   map[string]any{"port": l.dev.Port},
			Timestamp: time.Now().UTC(),
			Type:      "NODE_ALIVE",
		})
 
		err = l.readLoop(ctx, port)
		port.Close()
 
		if ctx.Err() != nil {
			return
		}
 
		log.Printf("[%s] Serial disconnected (%v) — reconnecting in 3s", l.dev.Name, err)
		l.bus.Publish(pubsub.Message{
			Topic:     "/node_death",
			Device:    l.dev.Name,
			Payload:   map[string]any{"reason": "serial disconnected"},
			Timestamp: time.Now().UTC(),
			Type:      "NODE_DEATH",
		})
 
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}
 
func (l *Listener) readLoop(ctx context.Context, port goserial.Port) error {
	scanner := bufio.NewScanner(port)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
 
		if !scanner.Scan() {
			return scanner.Err()
		}
 
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
 
		// Notify watchdog
		l.wd.Heartbeat(l.dev.Name)
 
		// Parse raw JSON from Arduino
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			l.handleSchemaError(line, "invalid JSON: "+err.Error())
			continue
		}
 
		// Validate schema
		topic, ok := raw["topic"].(string)
		if !ok || topic == "" {
			l.handleSchemaError(line, "missing or invalid 'topic' field")
			continue
		}
		if _, ok := raw["value"]; !ok {
			l.handleSchemaError(line, "missing 'value' field")
			continue
		}
 
		// Run against schema config
		if err := l.validator.Validate(topic, raw); err != nil {
			l.handleSchemaError(line, err.Error())
			continue
		}
 
		// Build canonical message
		msg := pubsub.Message{
			Topic:     topic,
			Device:    l.dev.Name,
			Payload:   raw,
			Timestamp: time.Now().UTC(),
			Type:      "MESSAGE",
		}
 
		// Persist to DB (synchronous — nothing ever lost)
		if err := l.db.InsertMessage(msg); err != nil {
			log.Printf("[%s] DB write error: %v", l.dev.Name, err)
		}
 
		// Publish to bus (async fan-out)
		l.bus.Publish(msg)
	}
}
 
func (l *Listener) handleSchemaError(raw, reason string) {
	log.Printf("[%s] SCHEMA_ERROR: %s | raw: %s", l.dev.Name, reason, raw)
 
	errMsg := pubsub.Message{
		Topic:  "/schema_error",
		Device: l.dev.Name,
		Payload: map[string]any{
			"raw":    raw,
			"reason": reason,
		},
		Timestamp: time.Now().UTC(),
		Type:      "SCHEMA_ERROR",
	}
 
	if err := l.db.InsertSchemaError(errMsg); err != nil {
		log.Printf("[%s] DB schema_error write error: %v", l.dev.Name, err)
	}
	l.bus.Publish(errMsg)
}