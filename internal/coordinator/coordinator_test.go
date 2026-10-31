package coordinator

import (
	"os"
	"testing"
)
func TestNewSplitsInput(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "input")
	if err != nil { t.Fatal(err) }
	if _, err := f.WriteString("a b c d e"); err != nil { t.Fatal(err) }
	if err := f.Close(); err != nil { t.Fatal(err) }
	c, err := New(f.Name(), "unused", 2)
	if err != nil { t.Fatal(err) }
	if len(c.maps) != 3 { t.Fatalf("expected 3 map tasks, got %d", len(c.maps)) }
}
func TestRejectsInvalidChunkSize(t *testing.T) {
	if _, err := New("missing", "out", 0); err == nil { t.Fatal("expected validation error") }
}