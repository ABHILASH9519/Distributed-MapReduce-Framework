package main

import (
	"flag"
	"log"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/worker"
)
func main() {
	addr := flag.String("coordinator", "127.0.0.1:9000", "coordinator RPC address")
	id := flag.String("id", "worker-1", "unique worker identifier")
	flag.Parse()
	if err := worker.Run(*addr, *id); err != nil { log.Fatal(err) }
}