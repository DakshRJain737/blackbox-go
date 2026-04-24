// Package ws implements the WebSocket hub that fans out pub/sub messages
// to every connected browser client in real time.
package ws
 
import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
 
	"github.com/gorilla/websocket"
 
	"blackbox/internal/pubsub"
)
 
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all origins for hackathon
	},
}
 
// client represents one connected browser.
type client struct {
	conn *websocket.Conn
	send chan []byte
	id   uint64
}
 
// Hub manages all WebSocket clients.
type Hub struct {
	mu      sync.Mutex
	clients map[uint64]*client
	bus     *pubsub.Bus
	nextID  uint64
}
 
// NewHub creates a new Hub.
func NewHub(bus *pubsub.Bus) *Hub {
	return &Hub{
		clients: make(map[uint64]*client),
		bus:     bus,
	}
}
 
// Run subscribes to all bus messages and fans them out to clients.
func (h *Hub) Run(ctx context.Context) {
	ch, unsub := h.bus.SubscribeAll(4096)
	defer unsub()
 
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()
 
	for {
		select {
		case <-ctx.Done():
			return
 
		case msg := <-ch:
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			h.broadcast(data)
 
		case <-heartbeat.C:
			ping := map[string]any{"type": "PING", "ts": time.Now().UTC()}
			data, _ := json.Marshal(ping)
			h.broadcast(data)
		}
	}
}
 
func (h *Hub) broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.clients {
		select {
		case c.send <- data:
		default:
			// Slow client — close it
			go h.removeClient(c.id)
		}
	}
}
 
// ServeHTTP upgrades an HTTP connection to WebSocket.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS upgrade error: %v", err)
		return
	}
 
	h.mu.Lock()
	h.nextID++
	c := &client{conn: conn, send: make(chan []byte, 512), id: h.nextID}
	h.clients[c.id] = c
	h.mu.Unlock()
 
	log.Printf("🔌 WS client %d connected (%s)", c.id, r.RemoteAddr)
 
	// Writer goroutine
	go func() {
		defer conn.Close()
		for data := range c.send {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				break
			}
		}
	}()
 
	// Reader goroutine (just drains to detect disconnects)
	go func() {
		defer func() {
			h.removeClient(c.id)
			conn.Close()
			log.Printf("🔌 WS client %d disconnected", c.id)
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}
 
func (h *Hub) removeClient(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.clients[id]; ok {
		close(c.send)
		delete(h.clients, id)
	}
}
 
// ConnectedCount returns the number of active WebSocket connections.
func (h *Hub) ConnectedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}
 