# Distributed MapReduce Framework (Go)

A small distributed word-count MapReduce system with a coordinator and independent worker processes communicating over Go's standard-library RPC.

## Run locally
```sh
go test ./...
go run ./cmd/coordinator -addr 127.0.0.1:9000 -input data/input.txt -output out.txt
# In another terminal:
go run ./cmd/worker -coordinator 127.0.0.1:9000 -id worker-1
```
The coordinator splits words into map tasks, workers compute intermediate counts, and a reduce task writes sorted output. Start multiple worker processes with distinct IDs to share work.

## Design and current limits
- RPC coordinator with task leases and retry after lease expiry
- Map and reduce phases with deterministic output ordering
- Worker identity is checked when accepting task completion
- Single coordinator and single reducer; no durable task state
- RPC is unauthenticated: bind to localhost or a trusted network only

This is an educational distributed-systems project, not a fault-tolerant production scheduler.