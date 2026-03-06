package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// RPC Server Wrapper
// ---------------------------------------------------------------------------

// RaftRPCServer wraps a RaftNode so its methods satisfy the net/rpc signature
// (must return error). net/rpc registers this object, not RaftNode directly.
type RaftRPCServer struct {
	node *RaftNode
}

// RequestVote adapts the RaftNode handler for net/rpc.
func (s *RaftRPCServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.node.RequestVote(args, reply)
	return nil
}

// AppendEntries adapts the RaftNode handler for net/rpc.
func (s *RaftRPCServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.node.AppendEntries(args, reply)
	return nil
}

// ---------------------------------------------------------------------------
// Transport — manages the RPC listener and outbound client pool
// ---------------------------------------------------------------------------

// Transport handles all inter-node communication for a RaftNode.
type Transport struct {
	node     *RaftNode
	listener net.Listener

	mu      sync.RWMutex
	clients map[int]*rpc.Client // peerID → cached client
}

// NewTransport creates a Transport bound to the given RaftNode.
func NewTransport(node *RaftNode) *Transport {
	return &Transport{
		node:    node,
		clients: make(map[int]*rpc.Client),
	}
}

// Start begins listening for inbound RPCs on the given address (e.g. ":9090").
func (t *Transport) Start(addr string) error {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &RaftRPCServer{node: t.node}); err != nil {
		return fmt.Errorf("transport: register RPC: %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("transport: listen %s: %w", addr, err)
	}
	t.listener = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// listener closed → stop accepting
				return
			}
			go server.ServeConn(conn)
		}
	}()

	log.Printf("[Transport] RPC server listening on %s", addr)
	return nil
}

// Stop closes the listener and all cached client connections.
func (t *Transport) Stop() {
	if t.listener != nil {
		t.listener.Close()
	}
	t.mu.Lock()
	for id, c := range t.clients {
		c.Close()
		delete(t.clients, id)
	}
	t.mu.Unlock()
	log.Println("[Transport] Stopped")
}

// getClient returns a cached *rpc.Client for the given peer, dialing if needed.
func (t *Transport) getClient(peerID int) (*rpc.Client, error) {
	t.mu.RLock()
	c, ok := t.clients[peerID]
	t.mu.RUnlock()
	if ok {
		return c, nil
	}

	t.node.mu.RLock()
	addr, exists := t.node.peerAddrs[peerID]
	t.node.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("no address for peer %d", peerID)
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	c = rpc.NewClient(conn)

	t.mu.Lock()
	t.clients[peerID] = c
	t.mu.Unlock()
	return c, nil
}

// dropClient removes a cached client (called on RPC failure so we reconnect).
func (t *Transport) dropClient(peerID int) {
	t.mu.Lock()
	if c, ok := t.clients[peerID]; ok {
		c.Close()
		delete(t.clients, peerID)
	}
	t.mu.Unlock()
}

// CallRequestVote sends a RequestVote RPC to a peer.
func (t *Transport) CallRequestVote(peerID int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	c, err := t.getClient(peerID)
	if err != nil {
		return false
	}
	if err := c.Call("Raft.RequestVote", args, reply); err != nil {
		t.dropClient(peerID)
		return false
	}
	return true
}

// CallAppendEntries sends an AppendEntries RPC to a peer.
func (t *Transport) CallAppendEntries(peerID int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	c, err := t.getClient(peerID)
	if err != nil {
		return false
	}
	if err := c.Call("Raft.AppendEntries", args, reply); err != nil {
		t.dropClient(peerID)
		return false
	}
	return true
}
