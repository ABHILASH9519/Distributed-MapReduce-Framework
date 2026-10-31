package main

import (
	"flag"
	"log"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/coordinator"
)
func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "RPC listen address")
	input := flag.String("input", "data/input.txt", "input text file")
	output := flag.String("output", "out.txt", "output file")
	chunk := flag.Int("chunk-size", 100, "words per map task")
	flag.Parse()
	c, err := coordinator.New(*input, *output, *chunk)
	if err != nil { log.Fatal(err) }
	log.Printf("coordinator listening on %s", *addr)
	if err := c.Serve(*addr); err != nil { log.Fatal(err) }
}