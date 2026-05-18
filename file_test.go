package cache_file

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileCacheReadEmptyValue(t *testing.T) {
	conn := &fileConnection{path: filepath.Join(t.TempDir(), "cache.db")}
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	if err := conn.Write("empty", []byte{}, time.Minute); err != nil {
		t.Fatalf("write: %v", err)
	}
	data, err := conn.Read("empty")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if data == nil {
		t.Fatal("expected empty value, got nil miss")
	}
	if len(data) != 0 {
		t.Fatalf("expected empty value, got %q", string(data))
	}
}

func TestFileCacheSequenceStartZero(t *testing.T) {
	conn := &fileConnection{path: filepath.Join(t.TempDir(), "cache.db")}
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	val, err := conn.Sequence("seq0", 0, 1, time.Minute)
	if err != nil {
		t.Fatalf("seq: %v", err)
	}
	if val != 0 {
		t.Fatalf("expected 0, got %d", val)
	}
	val, err = conn.Sequence("seq0", 0, 1, time.Minute)
	if err != nil {
		t.Fatalf("seq2: %v", err)
	}
	if val != 1 {
		t.Fatalf("expected 1, got %d", val)
	}
}

func TestFileCacheSequenceMany(t *testing.T) {
	conn := &fileConnection{path: filepath.Join(t.TempDir(), "cache.db")}
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	vals, err := conn.SequenceMany("seqMany", 10, 2, 3, time.Minute)
	if err != nil {
		t.Fatalf("sequence many: %v", err)
	}
	want := []int64{10, 12, 14}
	for i := range want {
		if vals[i] != want[i] {
			t.Fatalf("sequence many[%d]: expected %d, got %d", i, want[i], vals[i])
		}
	}
	next, err := conn.Sequence("seqMany", 10, 2, time.Minute)
	if err != nil {
		t.Fatalf("sequence next: %v", err)
	}
	if next != 16 {
		t.Fatalf("expected next 16, got %d", next)
	}
}

func TestFileCacheCloseAllowsReopen(t *testing.T) {
	conn := &fileConnection{path: filepath.Join(t.TempDir(), "cache.db")}
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := conn.Open(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}
}

func TestFileCacheKeysTreatPrefixLiterally(t *testing.T) {
	conn := &fileConnection{path: filepath.Join(t.TempDir(), "cache.db")}
	if err := conn.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer conn.Close()

	if err := conn.Write("a[1]", []byte("one"), time.Minute); err != nil {
		t.Fatalf("write one: %v", err)
	}
	if err := conn.Write("a1", []byte("two"), time.Minute); err != nil {
		t.Fatalf("write two: %v", err)
	}

	keys, err := conn.Keys("a[")
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "a[1]" {
		t.Fatalf("expected literal prefix match, got %v", keys)
	}

	if err := conn.Clear("a["); err != nil {
		t.Fatalf("clear: %v", err)
	}
	exists, err := conn.Exists("a1")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("clear with literal prefix removed unrelated key")
	}
}
