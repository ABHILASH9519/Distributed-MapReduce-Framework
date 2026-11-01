package coordinator

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/protocol"
)

// TestConcurrentWorkersCompleteJob exercises task leasing and completion with
// several workers requesting work concurrently.
func TestConcurrentWorkersCompleteJob(t *testing.T) {
	c, out := testCoordinator(t, "go map reduce go worker map distributed worker", 2)

	const workerCount = 3
	var wg sync.WaitGroup
	errs := make(chan error, workerCount)
	deadline := time.After(5 * time.Second)

	for i := 0; i < workerCount; i++ {
		workerID := "worker-" + string(rune('1'+i))
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				select {
				case <-deadline:
					errs <- errWorkerTimeout
					return
				default:
				}

				var next protocol.Response
				if err := c.Next(protocol.Request{WorkerID: id}, &next); err != nil {
					errs <- err
					return
				}
				if next.Done {
					return
				}
				if !next.HasTask {
					time.Sleep(5 * time.Millisecond)
					continue
				}

				counts := make(map[string]int)
				switch next.Task.Kind {
				case "map":
					for _, word := range next.Task.Data {
						counts[strings.ToLower(word)]++
					}
				case "reduce":
					counts = next.Task.Intermediate
				default:
					errs <- errUnknownTaskKind
					return
				}

				var completed protocol.CompleteResponse
				if err := c.Complete(protocol.CompleteRequest{
					WorkerID: id,
					TaskID: next.Task.ID,
					Kind: next.Task.Kind,
					LeaseToken: next.Task.LeaseToken,
					Counts: counts,
				}, &completed); err != nil {
					errs <- err
					return
				}
				if !completed.Accepted {
					errs <- errCompletionRejected
					return
				}
			}
		}(workerID)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(raw), "distributed 1\ngo 2\nmap 2\nreduce 1\nworker 2\n"; got != want {
		t.Fatalf("unexpected output: got %q, want %q", got, want)
	}
}

var (
	errWorkerTimeout = errors.New("workers did not finish before timeout")
	errUnknownTaskKind = errors.New("worker received an unknown task kind")
	errCompletionRejected = errors.New("coordinator rejected a valid worker completion")
)
