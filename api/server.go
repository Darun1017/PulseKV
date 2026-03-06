package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Darun1017/PulseKV/kvstore"
	"github.com/Darun1017/PulseKV/raft"
)

// ---------------------------------------------------------------------------
// Event & Watcher — SSE fan-out dispatcher
// ---------------------------------------------------------------------------

// Event represents a single change in the KVStore.
type Event struct {
	Type  string `json:"type"` // "PUT" or "DELETE"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Watcher manages a set of subscriber channels and fans out events
// without blocking the caller.
type Watcher struct {
	mu          sync.Mutex
	subscribers map[uint64]chan Event
	nextID      atomic.Uint64
}

// NewWatcher creates a ready-to-use Watcher.
func NewWatcher() *Watcher {
	return &Watcher{
		subscribers: make(map[uint64]chan Event),
	}
}

// Subscribe registers a new subscriber and returns its event channel.
// The channel is automatically deregistered when ctx is cancelled.
func (w *Watcher) Subscribe(ctx context.Context) <-chan Event {
	id := w.nextID.Add(1)
	ch := make(chan Event, 64) // buffered to avoid blocking the dispatcher

	w.mu.Lock()
	w.subscribers[id] = ch
	w.mu.Unlock()

	// Cleanup goroutine: deregister when the client disconnects.
	go func() {
		<-ctx.Done()
		w.mu.Lock()
		delete(w.subscribers, id)
		close(ch)
		w.mu.Unlock()
	}()

	return ch
}

// Notify sends an event to all subscribers. Drops if a subscriber's
// buffer is full (non-blocking) so it never stalls the writer.
func (w *Watcher) Notify(e Event) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, ch := range w.subscribers {
		select {
		case ch <- e:
		default:
			// slow consumer — drop to avoid blocking
		}
	}
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server is the HTTP front-end for PulseKV.
type Server struct {
	raft    *raft.RaftNode
	store   *kvstore.KVStore
	watcher *Watcher
	http    *http.Server
}

// NewServer creates a Server bound to the given address.
func NewServer(addr string, rn *raft.RaftNode, store *kvstore.KVStore, watcher *Watcher) *Server {
	s := &Server{
		raft:    rn,
		store:   store,
		watcher: watcher,
	}

	mux := http.NewServeMux()

	// Route registration — Go 1.22+ method-aware patterns.
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/watch", s.handleWatch)
	mux.HandleFunc("GET /v1/{key}", s.handleGet)
	mux.HandleFunc("PUT /v1/{key}", s.handlePut)
	mux.HandleFunc("DELETE /v1/{key}", s.handleDelete)

	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

// Start begins serving in a background goroutine.
func (s *Server) Start() {
	go func() {
		log.Printf("[API] Listening on %s", s.http.Addr)
		if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[API] ListenAndServe error: %v", err)
		}
	}()
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("[API] Shutting down HTTP server…")
	return s.http.Shutdown(ctx)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, `{"error":"key is required"}`, http.StatusBadRequest)
		return
	}

	value, ok := s.store.Get(key)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("key %q not found", key),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": value,
	})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, `{"error":"key is required"}`, http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MiB limit
	if err != nil {
		http.Error(w, `{"error":"failed to read body"}`, http.StatusBadRequest)
		return
	}
	value := strings.TrimSpace(string(body))
	if value == "" {
		http.Error(w, `{"error":"value is required (send as request body)"}`, http.StatusBadRequest)
		return
	}

	command := fmt.Sprintf("PUT %s %s", key, value)
	if ok := s.raft.Propose(command); !ok {
		s.rejectNotLeader(w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"key":    key,
		"value":  value,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, `{"error":"key is required"}`, http.StatusBadRequest)
		return
	}

	command := fmt.Sprintf("DELETE %s", key)
	if ok := s.raft.Propose(command); !ok {
		s.rejectNotLeader(w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"deleted": key,
	})
}

// handleStatus returns this node's current Raft status including who the leader is.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	term, role, _ := s.raft.GetState()
	leaderID := s.raft.GetLeaderID()

	leaderStr := "unknown"
	if leaderID >= 0 {
		leaderStr = fmt.Sprintf("%d", leaderID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id": s.raft.GetID(),
		"role":    role.String(),
		"term":    term,
		"leader":  leaderStr,
	})
}

// rejectNotLeader returns a JSON error indicating this node is not the leader.
func (s *Server) rejectNotLeader(w http.ResponseWriter) {
	leaderID := s.raft.GetLeaderID()
	nodeID := s.raft.GetID()

	leaderStr := "unknown"
	if leaderID >= 0 {
		leaderStr = fmt.Sprintf("%d", leaderID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTemporaryRedirect)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":     "not the leader",
		"node_id":   nodeID,
		"leader_id": leaderStr,
	})
}

// ---------------------------------------------------------------------------
// SSE Watch
// ---------------------------------------------------------------------------

func (s *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming not supported"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // nginx compatibility
	flusher.Flush()

	ctx := r.Context()
	events := s.watcher.Subscribe(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case ev, open := <-events:
			if !open {
				return
			}
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}
