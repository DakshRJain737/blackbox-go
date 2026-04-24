// Package watchdog monitors liveness of connected devices.
// If a device sends no heartbeat for DeadTimeout, a NODE_DEATH event is fired.
// When it comes back, NODE_ALIVE is fired.
package watchdog
 
import (
	"context"
	"log"
	"sync"
	"time"
 
	"blackbox/internal/pubsub"
)
 
const (
	DeadTimeout   = 3 * time.Second
	CheckInterval = 1 * time.Second
)
 
type nodeState struct {
	lastSeen time.Time
	alive    bool
}
 
// Watchdog tracks per-device heartbeats and broadcasts death/revival events.
type Watchdog struct {
	mu    sync.Mutex
	nodes map[string]*nodeState
	bus   *pubsub.Bus
}
 
// New creates a new Watchdog.
func New(bus *pubsub.Bus) *Watchdog {
	return &Watchdog{
		nodes: make(map[string]*nodeState),
		bus:   bus,
	}
}
 
// Heartbeat records that a device is alive right now.
func (w *Watchdog) Heartbeat(device string) {
	w.mu.Lock()
	defer w.mu.Unlock()
 
	state, ok := w.nodes[device]
	if !ok {
		w.nodes[device] = &nodeState{lastSeen: time.Now(), alive: true}
		return
	}
 
	wasAlive := state.alive
	state.lastSeen = time.Now()
	state.alive = true
 
	if !wasAlive {
		// Device just came back — fire NODE_ALIVE
		log.Printf("💚 Node %s is ALIVE again", device)
		w.bus.Publish(pubsub.Message{
			Topic:     "/node_alive",
			Device:    device,
			Payload:   map[string]any{"status": "alive"},
			Timestamp: time.Now().UTC(),
			Type:      "NODE_ALIVE",
		})
	}
}
 
// Run starts the watchdog ticker. Blocks until ctx is cancelled.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()
 
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.check()
		}
	}
}
 
func (w *Watchdog) check() {
	w.mu.Lock()
	defer w.mu.Unlock()
 
	now := time.Now()
	for device, state := range w.nodes {
		if state.alive && now.Sub(state.lastSeen) > DeadTimeout {
			state.alive = false
			log.Printf("💀 Node %s is DEAD (no heartbeat for %v)", device, DeadTimeout)
			w.bus.Publish(pubsub.Message{
				Topic:  "/node_death",
				Device: device,
				Payload: map[string]any{
					"last_seen": state.lastSeen.UTC(),
					"reason":    "heartbeat timeout",
				},
				Timestamp: now.UTC(),
				Type:      "NODE_DEATH",
			})
		}
	}
}
 
// NodeStatuses returns current alive/dead status of all known devices.
func (w *Watchdog) NodeStatuses() map[string]map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
 
	out := make(map[string]map[string]any, len(w.nodes))
	for device, state := range w.nodes {
		out[device] = map[string]any{
			"alive":     state.alive,
			"last_seen": state.lastSeen.UTC(),
		}
	}
	return out
}
 