package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Darun1017/PulseKV/kvstore"
	"github.com/Darun1017/PulseKV/raft"
)

func main() {
	// TODO (teammate – networking / config): read these from a config file or command-line flags.
	nodeID := 1
	peerIDs := []int{2, 3, 4}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Printf("PulseKV node %d starting up…", nodeID)

	store := kvstore.NewKVStore()
	applyCh := make(chan raft.LogEntry, 64)
	persister := raft.NewFilePersister(fmt.Sprintf("raft_state_%d.json", nodeID))
	node := raft.NewRaftNode(nodeID, peerIDs, applyCh, persister)

	node.Start()

	go func() {
		for entry := range applyCh {
			if err := store.Apply(entry.Command); err != nil {
				log.Printf("[Apply] ERROR applying entry %d (term %d): %v", entry.Index, entry.Term, err)
			} else {
				log.Printf("[Apply] Applied entry %d (term %d): %s", entry.Index, entry.Term, entry.Command)
			}
		}
	}()

	// TODO (teammate – HTTP API): start the HTTP server here, passing store and node endpoints.

	// TODO (teammate – networking): start the RPC server here for node-to-node communication.

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	fmt.Println()
	log.Printf("Received %v — shutting down…", sig)

	node.Stop()
	log.Println("PulseKV node shut down cleanly.")
}
