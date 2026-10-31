package worker

import (
	"net/rpc"
	"strings"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/protocol"
)
func Run(addr, id string) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil { return err }
	defer client.Close()
	for {
		var next protocol.Response
		if err := client.Call("Coordinator.Next", protocol.Request{WorkerID: id}, &next); err != nil { return err }
		if next.Done { return nil }
		if !next.HasTask { continue }
		counts := map[string]int{}
		if next.Task.Kind == "map" {
			for _, word := range next.Task.Data { counts[strings.ToLower(word)]++ }
		} else { counts = next.Task.Intermediate }
		req := protocol.CompleteRequest{WorkerID: id, TaskID: next.Task.ID, Kind: next.Task.Kind, Counts: counts}
		var completed protocol.CompleteResponse
		if err := client.Call("Coordinator.Complete", req, &completed); err != nil { return err }
	}
}