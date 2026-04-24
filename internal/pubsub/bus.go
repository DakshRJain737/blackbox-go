// Package pubsub implements a lightweight in-process pub/sub message bus.
// Topics mirror sensor streams, e.g. "/distance", "/tilt", "/temp".
// Special system topics: "/anomaly", "/schema_error", "/node_death", "/node_alive".
package pubsub
 
import (
	"context"
	"log"
	"sync"
	"time"
)
 
// Message is the canonical envelope for everything flowing through the broker.
type Message struct {
	Topic     string         `json:"topic"`
	Device    string         `json:"device"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
	Type      string         `json:"type"` // "MESSAGE" | "ANOMALY" | "SCHEMA_ERROR" | "NODE_DEATH" | "NODE_ALIVE"
}
 
type subscriber struct {
	ch   chan Message
	id   uint64
	done chan struct{}
}
 
// Bus is the central pub/sub engine.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[string][]*subscriber // topic → list of subs
	lastValue   map[string]Message       // topic → last known value
	inbox       chan Message
	nextID      uint64
}
 
// New creates a new Bus.
func New() *Bus {
	return &Bus{
		subscribers: make(map[string][]*subscriber),
		lastValue:   make(map[string]Message),
		inbox:       make(chan Message, 4096),
	}
}
 
// Run starts the dispatch loop. Must be called in a goroutine.
func (b *Bus) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-b.inbox:
			b.dispatch(msg)
		}
	}
}
 
// Publish enqueues a message for dispatch. Non-blocking.
func (b *Bus) Publish(msg Message) {
	select {
	case b.inbox <- msg:
	default:
		log.Printf("⚠️  pubsub inbox full, dropping message on topic %s", msg.Topic)
	}
}
 
// Subscribe returns a channel that receives all future messages on topic.
// The caller receives the last known value immediately if one exists.
func (b *Bus) Subscribe(topic string, bufSize int) (chan Message, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
 
	b.nextID++
	sub := &subscriber{
		ch:   make(chan Message, bufSize),
		id:   b.nextID,
		done: make(chan struct{}),
	}
 
	b.subscribers[topic] = append(b.subscribers[topic], sub)
 
	// Send last known value immediately (replay-on-subscribe)
	if last, ok := b.lastValue[topic]; ok {
		select {
		case sub.ch <- last:
		default:
		}
	}
 
	unsubFn := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		subs := b.subscribers[topic]
		for i, s := range subs {
			if s.id == sub.id {
				b.subscribers[topic] = append(subs[:i], subs[i+1:]...)
				close(sub.done)
				return
			}
		}
	}
 
	return sub.ch, unsubFn
}
 
// SubscribeAll returns a channel that receives every message on any topic.
func (b *Bus) SubscribeAll(bufSize int) (chan Message, func()) {
	return b.Subscribe("*", bufSize)
}
 
func (b *Bus) dispatch(msg Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
 
	// Store last value per topic
	b.lastValue[msg.Topic] = msg
 
	// Fan out to topic subscribers
	for _, sub := range b.subscribers[msg.Topic] {
		select {
		case sub.ch <- msg:
		default:
			// Slow subscriber — drop rather than block the whole bus
		}
	}
 
	// Fan out to wildcard subscribers
	if msg.Topic != "*" {
		for _, sub := range b.subscribers["*"] {
			select {
			case sub.ch <- msg:
			default:
			}
		}
	}
}
 
// Topics returns the list of topics that have at least one recorded value.
func (b *Bus) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	topics := make([]string, 0, len(b.lastValue))
	for t := range b.lastValue {
		topics = append(topics, t)
	}
	return topics
}
 
// LastValue returns the most recent message for a topic.
func (b *Bus) LastValue(topic string) (Message, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.lastValue[topic]
	return m, ok
}