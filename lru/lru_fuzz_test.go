package lru

import (
	"bytes"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

func FuzzEntryJSON(f *testing.F) {
	f.Add("hello", int64(42), time.Now().UnixNano(), time.Now().UnixNano(), uint64(7), true)
	f.Add("key/with/slashes", int64(1<<20), time.Now().UnixNano(), time.Now().UnixNano(), uint64(0), false)

	f.Fuzz(func(t *testing.T, key string, size int64, created int64, last int64, count uint64, access bool) {
		if size < 0 {
			size = -size
		}
		if !utf8.ValidString(key) {
			return
		}
		for _, r := range key {
			if !unicode.IsPrint(r) {
				return
			}
		}
		meta := EntryMeta{
			SizeBytes:    size,
			CreatedAt:    time.Unix(0, created).UTC(),
			LastAccessAt: time.Unix(0, last).UTC(),
			AccessCount:  count,
		}
		b, err := entryBytes(key, meta, access, time.Time{})
		if err != nil {
			t.Fatalf("entryBytes: %v", err)
		}
		e, err := parseEntry(b)
		if err != nil {
			t.Fatalf("parseEntry: %v", err)
		}
		if e.K != key {
			t.Errorf("key mismatch: got %q", e.K)
		}
		if e.M.SizeBytes != size {
			t.Errorf("size mismatch")
		}
		if e.M.AccessCount != count {
			t.Errorf("count mismatch")
		}
		if e.Access != access {
			t.Errorf("access mismatch")
		}
		d := e.M.CreatedAt.Sub(meta.CreatedAt)
		if d < 0 {
			d = -d
		}
		if d > time.Microsecond {
			t.Errorf("created mismatch")
		}
	})
}

func TestEntryJSONRoundTrip(t *testing.T) {
	meta := EntryMeta{
		SizeBytes:    12345,
		CreatedAt:    time.Now().UTC(),
		LastAccessAt: time.Now().UTC(),
		AccessCount:  99,
	}
	b, err := entryBytes("my/key", meta, true, time.Time{})
	if err != nil {
		t.Fatalf("entryBytes: %v", err)
	}
	if !bytes.Contains(b, []byte(`"a":true`)) {
		t.Errorf("expected access bit in JSON")
	}
	e, err := parseEntry(b)
	if err != nil {
		t.Fatalf("parseEntry: %v", err)
	}
	if e.M.SizeBytes != 12345 {
		t.Errorf("size mismatch")
	}
}
