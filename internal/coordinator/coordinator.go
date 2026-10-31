package coordinator

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/protocol"
)

type taskState struct {
	task     protocol.Task
	done     bool
	worker   string
	leasedAt time.Time
}
type Coordinator struct {
	mu           sync.Mutex
	maps         []taskState
	reduces      []taskState
	intermediate map[string]int
	output       string
}
func New(inputPath, output string, chunkSize int) (*Coordinator, error) {
	if chunkSize < 1 { return nil, errors.New("chunk size must be positive") }
	raw, err := os.ReadFile(inputPath)
	if err != nil { return nil, err }
	words := strings.Fields(string(raw))
	c := &Coordinator{output: output, intermediate: map[string]int{}}
	for i := 0; i < len(words); i += chunkSize {
		end := i + chunkSize
		if end > len(words) { end = len(words) }
		c.maps = append(c.maps, taskState{task: protocol.Task{ID: len(c.maps), Kind: "map", Data: words[i:end]}})
	}
	if len(c.maps) == 0 { c.maps = append(c.maps, taskState{task: protocol.Task{ID: 0, Kind: "map"}}) }
	return c, nil
}
func (c *Coordinator) Next(req protocol.Request, res *protocol.Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.maps {
		s := &c.maps[i]
		if !s.done && (s.worker == "" || time.Since(s.leasedAt) > 10*time.Second) {
			s.worker, s.leasedAt = req.WorkerID, time.Now()
			res.Task, res.HasTask = s.task, true
			return nil
		}
	}
	allMaps := true
	for _, s := range c.maps { if !s.done { allMaps = false } }
	if allMaps && len(c.reduces) == 0 {
		copyCounts := make(map[string]int, len(c.intermediate))
		for k, v := range c.intermediate { copyCounts[k] = v }
		c.reduces = []taskState{{task: protocol.Task{ID: 0, Kind: "reduce", Intermediate: copyCounts}}}
	}
	for i := range c.reduces {
		s := &c.reduces[i]
		if !s.done && (s.worker == "" || time.Since(s.leasedAt) > 10*time.Second) {
			s.worker, s.leasedAt = req.WorkerID, time.Now()
			res.Task, res.HasTask = s.task, true
			return nil
		}
	}
	res.Done = allMaps && len(c.reduces) > 0 && c.reduces[0].done
	return nil
}
func (c *Coordinator) Complete(req protocol.CompleteRequest, res *protocol.CompleteResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var tasks *[]taskState
	switch req.Kind {
	case "map": tasks = &c.maps
	case "reduce": tasks = &c.reduces
	default: return errors.New("unknown task kind")
	}
	if req.TaskID < 0 || req.TaskID >= len(*tasks) { return errors.New("invalid task id") }
	s := &(*tasks)[req.TaskID]
	if s.done || s.worker != req.WorkerID { return errors.New("task lease mismatch or already completed") }
	if req.Kind == "map" {
		for k, v := range req.Counts { c.intermediate[k] += v }
	} else {
		keys := make([]string, 0, len(req.Counts))
		for k := range req.Counts { keys = append(keys, k) }
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys { b.WriteString(k); b.WriteByte(' '); b.WriteString(strconv.Itoa(req.Counts[k])); b.WriteByte('\n') }
		if err := os.WriteFile(c.output, []byte(b.String()), 0644); err != nil { return err }
	}
	s.done, res.Accepted = true, true
	return nil
}
func (c *Coordinator) Serve(addr string) error {
	server := rpc.NewServer()
	if err := server.RegisterName("Coordinator", c); err != nil { return err }
	listener, err := net.Listen("tcp", addr)
	if err != nil { return err }
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil { return err }
		go server.ServeConn(conn)
	}
}