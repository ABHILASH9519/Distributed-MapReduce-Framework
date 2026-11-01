package coordinator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ABHILASH9519/Distributed-MapReduce-Framework/internal/protocol"
)

func testCoordinator(t *testing.T, input string, chunk int) (*Coordinator,string) {
	t.Helper()
	dir:=t.TempDir()
	in,out:=filepath.Join(dir,"input.txt"),filepath.Join(dir,"output.txt")
	if err:=os.WriteFile(in,[]byte(input),0600); err!=nil { t.Fatal(err) }
	c,err:=New(in,out,chunk)
	if err!=nil { t.Fatal(err) }
	return c,out
}
func TestNewSplitsInput(t *testing.T) {
	c,_:=testCoordinator(t,"a b c d e",2)
	if len(c.maps)!=3 { t.Fatalf("expected 3 map tasks, got %d",len(c.maps)) }
	if got:=strings.Join(c.maps[2].task.Data," "); got!="e" { t.Fatalf("unexpected last chunk %q",got) }
}
func TestRejectsInvalidChunkSize(t *testing.T) {
	if _,err:=New("missing","out",0); err==nil { t.Fatal("expected validation error") }
}
func TestLeaseExpiryFencesStaleWorker(t *testing.T) {
	c,_:=testCoordinator(t,"go map",2)
	c.leaseDuration=20*time.Millisecond
	var a,b protocol.Response
	if err:=c.Next(protocol.Request{WorkerID:"a"},&a); err!=nil { t.Fatal(err) }
	time.Sleep(30*time.Millisecond)
	if err:=c.Next(protocol.Request{WorkerID:"b"},&b); err!=nil { t.Fatal(err) }
	if !b.HasTask || a.Task.LeaseToken==b.Task.LeaseToken { t.Fatal("expected a fresh lease token") }
	var rejected protocol.CompleteResponse
	if err:=c.Complete(protocol.CompleteRequest{WorkerID:"a",TaskID:a.Task.ID,Kind:"map",LeaseToken:a.Task.LeaseToken,Counts:map[string]int{"go":1}},&rejected); err==nil { t.Fatal("stale completion was accepted") }
	var accepted protocol.CompleteResponse
	if err:=c.Complete(protocol.CompleteRequest{WorkerID:"b",TaskID:b.Task.ID,Kind:"map",LeaseToken:b.Task.LeaseToken,Counts:map[string]int{"go":1}},&accepted); err!=nil { t.Fatal(err) }
	if !accepted.Accepted { t.Fatal("completion not accepted") }
}
func TestJobProducesSortedOutput(t *testing.T) {
	c,out:=testCoordinator(t,"Go go MAP map go",2)
	for {
		var next protocol.Response
		if err:=c.Next(protocol.Request{WorkerID:"test"},&next); err!=nil { t.Fatal(err) }
		if next.Done { break }
		if !next.HasTask { t.Fatal("expected task for single worker") }
		counts:=map[string]int{}
		if next.Task.Kind=="map" { for _,w:=range next.Task.Data { counts[strings.ToLower(w)]++ } } else { counts=next.Task.Intermediate }
		var done protocol.CompleteResponse
		if err:=c.Complete(protocol.CompleteRequest{WorkerID:"test",TaskID:next.Task.ID,Kind:next.Task.Kind,LeaseToken:next.Task.LeaseToken,Counts:counts},&done); err!=nil { t.Fatal(err) }
	}
	raw,err:=os.ReadFile(out)
	if err!=nil { t.Fatal(err) }
	if got,want:=string(raw),"go 3\nmap 2\n"; got!=want { t.Fatalf("got %q, want %q",got,want) }
}
func TestEmptyInputCreatesEmptyOutput(t *testing.T) {
	c,out:=testCoordinator(t,"",2)
	var next protocol.Response
	if err:=c.Next(protocol.Request{WorkerID:"worker"},&next); err!=nil { t.Fatal(err) }
	if !next.HasTask || next.Task.Kind!="reduce" { t.Fatal("expected empty reduce task") }
	var done protocol.CompleteResponse
	if err:=c.Complete(protocol.CompleteRequest{WorkerID:"worker",TaskID:0,Kind:"reduce",LeaseToken:next.Task.LeaseToken,Counts:map[string]int{}},&done); err!=nil { t.Fatal(err) }
	info,err:=os.Stat(out)
	if err!=nil { t.Fatal(err) }
	if info.Size()!=0 { t.Fatalf("expected empty output, got %d bytes",info.Size()) }
}
