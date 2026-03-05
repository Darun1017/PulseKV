package raft

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

// Persister holds the saved Raft state and snapshots.
// In a real system, this would write to a stable storage medium like a disk.
type Persister interface {
	SaveRaftState(state []byte)
	ReadRaftState() []byte
	SaveStateAndSnapshot(state []byte, snapshot []byte)
	ReadSnapshot() []byte
	SnapshotSize() int
}

// FilePersister implements Persister by writing to a local JSON file.
type FilePersister struct {
	mu           sync.RWMutex
	raftState    []byte
	snapshotData []byte
	filename     string
}

func NewFilePersister(filename string) *FilePersister {
	fp := &FilePersister{
		filename: filename,
	}
	// Try to load existing state
	data, err := os.ReadFile(filename)
	if err == nil {
		var state struct {
			RaftState    []byte `json:"raft_state"`
			SnapshotData []byte `json:"snapshot_data"`
		}
		if err := json.Unmarshal(data, &state); err == nil {
			fp.raftState = state.RaftState
			fp.snapshotData = state.SnapshotData
		}
	}
	return fp
}

func (fp *FilePersister) saveToFile() {
	state := struct {
		RaftState    []byte `json:"raft_state"`
		SnapshotData []byte `json:"snapshot_data"`
	}{
		RaftState:    fp.raftState,
		SnapshotData: fp.snapshotData,
	}

	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("[Persister] Failed to marshal state: %v", err)
		return
	}

	// Write to a temporary file, then rename atomically
	tmpFilename := fp.filename + ".tmp"
	if err := os.WriteFile(tmpFilename, data, 0644); err != nil {
		log.Printf("[Persister] Failed to write temporary state: %v", err)
		return
	}

	if err := os.Rename(tmpFilename, fp.filename); err != nil {
		log.Printf("[Persister] Failed to rename temporary state: %v", err)
	}
}

func (fp *FilePersister) SaveRaftState(state []byte) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.raftState = append([]byte(nil), state...)
	fp.saveToFile()
}

func (fp *FilePersister) ReadRaftState() []byte {
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	return append([]byte(nil), fp.raftState...)
}

func (fp *FilePersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	fp.raftState = append([]byte(nil), state...)
	fp.snapshotData = append([]byte(nil), snapshot...)
	fp.saveToFile()
}

func (fp *FilePersister) ReadSnapshot() []byte {
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	return append([]byte(nil), fp.snapshotData...)
}

func (fp *FilePersister) SnapshotSize() int {
	fp.mu.RLock()
	defer fp.mu.RUnlock()
	return len(fp.snapshotData)
}

// MemoryPersister implements Persister mainly for tests.
type MemoryPersister struct {
	mu           sync.RWMutex
	raftState    []byte
	snapshotData []byte
}

func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

func (mp *MemoryPersister) SaveRaftState(state []byte) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.raftState = append([]byte(nil), state...)
}

func (mp *MemoryPersister) ReadRaftState() []byte {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return append([]byte(nil), mp.raftState...)
}

func (mp *MemoryPersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.raftState = append([]byte(nil), state...)
	mp.snapshotData = append([]byte(nil), snapshot...)
}

func (mp *MemoryPersister) ReadSnapshot() []byte {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return append([]byte(nil), mp.snapshotData...)
}

func (mp *MemoryPersister) SnapshotSize() int {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return len(mp.snapshotData)
}
