package worker

import (
	"errors"
	"fmt"
	"net/rpc"
	"strings"
	"time"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/protocol"
)

const idlePollInterval = 100 * time.Millisecond

func Run(addr, id string) error {
	if strings.TrimSpace(id) == "" { return errors.New("worker id must not be empty") }
	client, err := rpc.Dial("tcp", addr)
	if err != nil { return fmt.Errorf("connect to coordinator: %w", err) }
	defer client.Close()
	for {
		var next protocol.Response
		if err := client.Call("Coordinator.Next", protocol.Request{WorkerID:id}, &next); err != nil { return fmt.Errorf("request task: %w", err) }
		if next.Done { return nil }
		if !next.HasTask { time.Sleep(idlePollInterval); continue }
		counts := make(map[string]int)
		switch next.Task.Kind {
		case "map":
			for _, word := range next.Task.Data { counts[strings.ToLower(word)]++ }
		case "reduce":
			counts = next.Task.Intermediate
		default:
			return fmt.Errorf("unknown task kind %q", next.Task.Kind)
		}
		req := protocol.CompleteRequest{WorkerID:id, TaskID:next.Task.ID, Kind:next.Task.Kind, LeaseToken:next.Task.LeaseToken, Counts:counts}
		var completed protocol.CompleteResponse
		if err := client.Call("Coordinator.Complete", req, &completed); err != nil { return fmt.Errorf("complete task: %w", err) }
		if !completed.Accepted { return errors.New("coordinator rejected task completion") }
	}
}
