package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Darun1017/PulseKV/api"
	"github.com/Darun1017/PulseKV/kvstore"
	"github.com/Darun1017/PulseKV/raft"
)

func main() {
	// ---------------------------------------------------------------------------
	// CLI flags
	// ---------------------------------------------------------------------------
	nodeID := flag.Int("id", 1, "Unique numeric node ID")
	httpPort := flag.Int("http-port", 8080, "Port for the client HTTP API")
	rpcPort := flag.Int("rpc-port", 9090, "Port for Raft inter-node RPCs")
	peersFlag := flag.String("peers", "", "Comma-separated peer list: id=host:rpcPort,... (e.g. 2=192.168.1.102:9090,3=192.168.1.103:9090)")
	flag.Parse()

	// Parse --peers into peerIDs + peerAddrs.
	peerIDs, peerAddrs := parsePeers(*peersFlag)
	httpAddr := fmt.Sprintf(":%d", *httpPort)
	rpcAddr := fmt.Sprintf(":%d", *rpcPort)

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Printf("PulseKV node %d starting up…", *nodeID)

	store := kvstore.NewKVStore()
	applyCh := make(chan raft.LogEntry, 64)
	persister := raft.NewFilePersister(fmt.Sprintf("raft_state_%d.json", *nodeID))
	node := raft.NewRaftNode(*nodeID, peerIDs, applyCh, persister)

	// Demo-friendly timings (slower so humans can observe the logs).
	// For production, remove these 3 lines to use the fast defaults.
	node.ElectionTimeoutBase = 3 * time.Second
	node.ElectionTimeoutSpread = 3000 // 3-6s total
	node.HeartbeatInterval = 1 * time.Second

	// Configure peer addresses and start the RPC transport.
	node.SetPeerAddrs(peerAddrs)
	transport := raft.NewTransport(node)
	node.SetTransport(transport)

	if err := transport.Start(rpcAddr); err != nil {
		log.Fatalf("Failed to start RPC transport: %v", err)
	}

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

	transport.Stop()
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

// parsePeers parses a comma-separated peer list like "2=192.168.1.102:9090,3=192.168.1.103:9090"
// into a slice of peer IDs and an address map.
func parsePeers(raw string) ([]int, map[int]string) {
	addrs := make(map[int]string)
	var ids []int
	if raw == "" {
		return ids, addrs
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "Invalid peer entry: %q (expected id=host:port)\n", entry)
			os.Exit(1)
		}
		id, err := strconv.Atoi(parts[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid peer ID %q: %v\n", parts[0], err)
			os.Exit(1)
		}
		ids = append(ids, id)
		addrs[id] = parts[1]
	}
	return ids, addrs
}
