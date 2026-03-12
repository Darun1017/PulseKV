# PulseKV: Consistent Distributed Key-Value Store

PulseKV is a distributed, strongly consistent Key-Value store built from scratch in Go. It implements the **Raft Consensus Algorithm** to ensure data reliability and consistency across a cluster of nodes.

## 🚀 Features

### Core Distributed Logic
- **Raft Consensus:** Full implementation of Leader Election, Log Replication, and Safety.
- **Persistence:** Raft state and logs are persisted to disk to handle node failures and restarts.
- **Log Compaction:** Automatic snapshotting to prevent logs from growing indefinitely.
- **Consistency:** Strong consistency guarantees (Linearizability) for all operations.

### API & Storage
- **HTTP REST API:** Simple interface for `GET`, `PUT`, and `DELETE` operations.
- **In-Memory Store:** High-performance key-value storage engine.
- **Real-time Watcher:** Server-Sent Events (SSE) stream allowing clients to subscribe to key changes in real-time.

### Visual Dashboard (Web UI)
- **Premium Interface:** A modern web-based dashboard with a dark-mode, glassmorphism design.
- **Cluster Monitoring:** Live view of Node ID, Role (Leader/Follower), Term, and current Leader.
- **Live State View:** Real-time table of the Key-Value store and an event log indicating all cluster activity.

---

## 🛠️ Tech Stack
- **Backend:** Go (Golang)
- **Communication:** Custom RPC for internal Raft traffic, HTTP for client API.
- **Frontend:** Vanilla HTML5, CSS3 (Modern Glassmorphism), JavaScript (ES6+).

---

## 🏃 Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/Darun1017/PulseKV.git
cd PulseKV
```

### 2. Start a 3-Node Cluster
Open three separate terminals and run the following commands:

**Terminal 1 (Node 1):**
```bash
go run main.go -id 1 -http-port 8080 -rpc-port 9090 -peers "2=localhost:9091,3=localhost:9092"
```

**Terminal 2 (Node 2):**
```bash
go run main.go -id 2 -http-port 8081 -rpc-port 9091 -peers "1=localhost:9090,3=localhost:9092"
```

**Terminal 3 (Node 3):**
```bash
go run main.go -id 3 -http-port 8082 -rpc-port 9092 -peers "1=localhost:9090,2=localhost:9091"
```

### 3. Access the Web Dashboard
Open your browser and visit any of the node addresses:
- Node 1: [http://localhost:8080](http://localhost:8080)
- Node 2: [http://localhost:8081](http://localhost:8081)
- Node 3: [http://localhost:8082](http://localhost:8082)

---

## 📂 Project Structure
- `api/`: HTTP API server and SSE Watcher implementation.
- `raft/`: The core Raft consensus engine (Election, Replication, Persistence).
- `kvstore/`: The state machine logic for the in-memory key-value store.
- `ui/`: Modern web interface assets.
- `main.go`: Application entry point and CLI flag handling.
