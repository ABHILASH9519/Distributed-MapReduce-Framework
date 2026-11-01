# Distributed MapReduce Framework

A distributed word-count engine implemented in **Go**, using independent worker processes and Go's standard-library RPC.

## Features

- Coordinator splits input into map tasks.
- Multiple workers request and execute tasks concurrently.
- Expired leases allow abandoned work to be reassigned.
- Unique lease tokens fence stale workers after reassignment.
- Worker identity, task ID, task kind, and lease token are validated on completion.
- Reduce output is sorted and atomically published using a temporary file and rename.
- Empty input is supported and produces an empty output file.
- No third-party Go dependencies.

## Requirements

- Go 1.22 or newer.

## Run locally

Start the coordinator:

    go run ./cmd/coordinator -addr 127.0.0.1:9000 -input data/input.txt -output out.txt -chunk-size 100

Start a worker in another terminal:

    go run ./cmd/worker -coordinator 127.0.0.1:9000 -id worker-1

Start additional worker processes with unique IDs (worker-2, worker-3, etc.). Workers exit when the coordinator reports that the job is complete.

## Verify

    gofmt -w .
    go vet ./...
    go test -race ./...

## Architecture

    Input file
       |
       v
    Coordinator -- leases map tasks --> Worker 1
         |                            --> Worker 2
         ^                                 |
         |--------- word counts -----------|
         |
         +-- after all maps complete --> Reduce task
                                         |
                                         v
                                   Sorted output file

## Task lifecycle

1. A worker requests a task using its unique worker ID.
2. The coordinator grants a time-limited lease and a fresh fencing token.
3. The worker computes the task and submits the result with that token.
4. The coordinator accepts only the current lease holder; stale completions are rejected.
5. Expired unfinished tasks can be leased to another worker.
6. Reduce starts only after every map task is complete.

## Scope and limitations

This is an educational distributed-systems implementation, not a production Hadoop replacement. It currently uses one in-memory coordinator and one reducer. Job state is not durable across coordinator restarts, RPC has no authentication or encryption, and intermediate data is held in coordinator memory. Keep RPC bound to localhost or a trusted private network. A production deployment would need durable state, authenticated transport, partitioned reducers, resource limits, and operational observability.
