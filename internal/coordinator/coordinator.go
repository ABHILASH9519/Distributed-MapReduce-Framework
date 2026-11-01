package coordinator

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/protocol"
)

const defaultLeaseDuration = 10 * time.Second

type taskState struct {
	task protocol.Task
	done bool
	worker string
	leaseToken string
	leasedAt time.Time
	attempts int
}
type Coordinator struct {
	mu sync.Mutex
	maps []taskState
	reduces []taskState
	intermediate map[string]int
	output string
	leaseDuration time.Duration
}

func New(inputPath, output string, chunkSize int) (*Coordinator, error) {
	if chunkSize < 1 { return nil, errors.New("chunk size must be positive") }
	if strings.TrimSpace(output) == "" { return nil, errors.New("output path must not be empty") }
	raw, err := os.ReadFile(inputPath)
	if err != nil { return nil, fmt.Errorf("read input: %w", err) }
	words := strings.Fields(string(raw))
	c := &Coordinator{output:output, intermediate:make(map[string]int), leaseDuration:defaultLeaseDuration}
	for i:=0; i<len(words); i+=chunkSize {
		end:=i+chunkSize
		if end>len(words) { end=len(words) }
		c.maps=append(c.maps, taskState{task:protocol.Task{ID:len(c.maps), Kind:"map", Data:append([]string(nil), words[i:end]...)}})
	}
	return c,nil
}

func leaseToken() (string,error) {
	var b [16]byte
	if _,err:=rand.Read(b[:]); err!=nil { return "",fmt.Errorf("generate lease token: %w",err) }
	return hex.EncodeToString(b[:]),nil
}
func (c *Coordinator) lease(tasks []taskState, worker string, res *protocol.Response) (bool,error) {
	now:=time.Now()
	for i:=range tasks {
		s:=&tasks[i]
		if s.done || (s.worker!="" && now.Sub(s.leasedAt)<=c.leaseDuration) { continue }
		token,err:=leaseToken()
		if err!=nil { return false,err }
		s.worker,s.leaseToken,s.leasedAt=worker,token,now
		s.attempts++
		task:=s.task
		task.LeaseToken=token
		res.Task,res.HasTask=task,true
		return true,nil
	}
	return false,nil
}
func (c *Coordinator) Next(req protocol.Request,res *protocol.Response) error {
	if strings.TrimSpace(req.WorkerID)=="" { return errors.New("worker id must not be empty") }
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok,err:=c.lease(c.maps,req.WorkerID,res); err!=nil || ok { return err }
	for _,s:=range c.maps { if !s.done { return nil } }
	if len(c.reduces)==0 {
		counts:=make(map[string]int,len(c.intermediate))
		for word,count:=range c.intermediate { counts[word]=count }
		c.reduces=[]taskState{{task:protocol.Task{ID:0,Kind:"reduce",Intermediate:counts}}}
	}
	if ok,err:=c.lease(c.reduces,req.WorkerID,res); err!=nil || ok { return err }
	res.Done=len(c.reduces)>0 && c.reduces[0].done
	return nil
}
func (c *Coordinator) Complete(req protocol.CompleteRequest,res *protocol.CompleteResponse) error {
	if strings.TrimSpace(req.WorkerID)=="" || req.LeaseToken=="" { return errors.New("worker id and lease token are required") }
	c.mu.Lock()
	defer c.mu.Unlock()
	var tasks *[]taskState
	switch req.Kind { case "map": tasks=&c.maps; case "reduce": tasks=&c.reduces; default: return errors.New("unknown task kind") }
	if req.TaskID<0 || req.TaskID>=len(*tasks) { return errors.New("invalid task id") }
	s:=&(*tasks)[req.TaskID]
	if s.done || s.worker!=req.WorkerID || s.leaseToken!=req.LeaseToken || time.Since(s.leasedAt)>c.leaseDuration { return errors.New("task lease is stale, mismatched, or already completed") }
	for word,count:=range req.Counts { if strings.TrimSpace(word)=="" || count<0 { return errors.New("invalid word or negative count in task result") } }
	if req.Kind=="map" {
		for word,count:=range req.Counts { c.intermediate[word]+=count }
	} else if err:=c.writeOutput(req.Counts); err!=nil { return err }
	s.done=true
	res.Accepted=true
	return nil
}
func (c *Coordinator) writeOutput(counts map[string]int) error {
	keys:=make([]string,0,len(counts))
	for word:=range counts { keys=append(keys,word) }
	sort.Strings(keys)
	var b strings.Builder
	for _,word:=range keys { fmt.Fprintf(&b,"%s %d\n",word,counts[word]) }
	tmp,err:=os.CreateTemp(filepath.Dir(c.output),".mapreduce-*.tmp")
	if err!=nil { return fmt.Errorf("create output temp file: %w",err) }
	name:=tmp.Name()
	defer os.Remove(name)
	if _,err=tmp.WriteString(b.String()); err!=nil { tmp.Close(); return fmt.Errorf("write output: %w",err) }
	if err=tmp.Sync(); err!=nil { tmp.Close(); return fmt.Errorf("sync output: %w",err) }
	if err=tmp.Close(); err!=nil { return fmt.Errorf("close output: %w",err) }
	if err=os.Rename(name,c.output); err!=nil { return fmt.Errorf("publish output: %w",err) }
	return nil
}
func (c *Coordinator) Serve(addr string) error {
	listener,err:=net.Listen("tcp",addr)
	if err!=nil { return fmt.Errorf("listen on %s: %w",addr,err) }
	return c.ServeListener(listener)
}
func (c *Coordinator) ServeListener(listener net.Listener) error {
	server:=rpc.NewServer()
	if err:=server.RegisterName("Coordinator",c); err!=nil { return err }
	for {
		conn,err:=listener.Accept()
		if err!=nil { if errors.Is(err,net.ErrClosed) { return nil }; return fmt.Errorf("accept RPC connection: %w",err) }
		go server.ServeConn(conn)
	}
}
