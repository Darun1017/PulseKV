package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Darun1017/PulseKV/api"
	"github.com/Darun1017/PulseKV/kvstore"
	"github.com/Darun1017/PulseKV/raft"
)

func main() {
	// TODO (teammate – networking / config): read these from a config file or command-line flags.
	nodeID := 1
	peerIDs := []int{2, 3, 4}
	httpAddr := ":8080"

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Printf("PulseKV node %d starting up…", nodeID)

	store := kvstore.NewKVStore()
	applyCh := make(chan raft.LogEntry, 64)
	persister := raft.NewFilePersister(fmt.Sprintf("raft_state_%d.json", nodeID))
	node := raft.NewRaftNode(nodeID, peerIDs, applyCh, persister)

	// Watcher dispatches SSE events to connected clients.
	watcher := api.NewWatcher()

	node.Start()

	// Apply committed log entries to the KVStore and notify watchers.
	go func() {
		for entry := range applyCh {
			if err := store.Apply(entry.Command); err != nil {
				log.Printf("[Apply] ERROR applying entry %d (term %d): %v", entry.Index, entry.Term, err)
				continue
			}
			log.Printf("[Apply] Applied entry %d (term %d): %s", entry.Index, entry.Term, entry.Command)

			// Parse the command to build a watch Event.
			event := parseEvent(entry.Command)
			if event != nil {
				watcher.Notify(*event)
			}
		}
	}()

	// Start the HTTP API server.
	srv := api.NewServer(httpAddr, node, store, watcher)
	srv.Start()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	fmt.Println()
	log.Printf("Received %v — shutting down…", sig)

	// Graceful HTTP shutdown with 5-second deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[Shutdown] HTTP server error: %v", err)
	}

	node.Stop()
	log.Println("PulseKV node shut down cleanly.")
}

// parseEvent converts a Raft command string into a watch Event.
func parseEvent(command string) *api.Event {
	parts := strings.SplitN(command, " ", 3)
	if len(parts) < 2 {
		return nil
	}
	switch parts[0] {
	case "PUT":
		e := api.Event{Type: "PUT", Key: parts[1]}
		if len(parts) == 3 {
			e.Value = parts[2]
		}
		return &e
	case "DELETE":
		return &api.Event{Type: "DELETE", Key: parts[1]}
	default:
		return nil
	}
}
