package lru

import (
	"encoding/json"
	"errors"
	"time"
)

// entryVersion is the on-wire version of the LRU entry JSON value.
const entryVersion = 1

// Entry is the public view of a live metadata record.
type Entry struct {
	Key      string
	Meta     EntryMeta
	Revision uint64
}

// EntryMeta describes a cached object for capacity and eviction decisions.
type EntryMeta struct {
	SizeBytes    int64
	CreatedAt    time.Time
	LastAccessAt time.Time
	AccessCount  uint64
}

// entry is the internal JSON value stored inside the cas envelope.
type entry struct {
	V       int       `json:"v"`
	K       string    `json:"k"`
	M       meta      `json:"m"`
	Access  bool      `json:"a"`
	Cleared time.Time `json:"cleared,omitempty"`
}

type meta struct {
	SizeBytes    int64     `json:"size"`
	CreatedAt    time.Time `json:"created"`
	LastAccessAt time.Time `json:"last"`
	AccessCount  uint64    `json:"count"`
}

// entryBytes marshals an entry value. now is the current time used when
// updating LastAccessAt; cleared is written only when non-zero.
func entryBytes(key string, em EntryMeta, access bool, cleared time.Time) ([]byte, error) {
	e := entry{
		V:      entryVersion,
		K:      key,
		Access: access,
		M: meta{
			SizeBytes:    em.SizeBytes,
			CreatedAt:    em.CreatedAt.UTC(),
			LastAccessAt: em.LastAccessAt.UTC(),
			AccessCount:  em.AccessCount,
		},
	}
	if !cleared.IsZero() {
		e.Cleared = cleared.UTC()
	}
	return json.Marshal(e)
}

// parseEntry decodes an entry value produced by entryBytes.
func parseEntry(value []byte) (entry, error) {
	var raw struct {
		V       int    `json:"v"`
		K       string `json:"k"`
		M       meta   `json:"m"`
		Access  bool   `json:"a"`
		Cleared string `json:"cleared"`
	}
	dec := json.NewDecoder(newBytesReader(value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return entry{}, err
	}
	if raw.V != entryVersion {
		return entry{}, errors.New("lru: unsupported entry version")
	}
	out := entry{
		V:      raw.V,
		K:      raw.K,
		Access: raw.Access,
		M:      raw.M,
	}
	if raw.Cleared != "" {
		t, err := time.Parse(time.RFC3339Nano, raw.Cleared)
		if err != nil {
			return entry{}, err
		}
		out.Cleared = t
	}
	return out, nil
}

func (m meta) toPublic() EntryMeta {
	return EntryMeta{
		SizeBytes:    m.SizeBytes,
		CreatedAt:    m.CreatedAt,
		LastAccessAt: m.LastAccessAt,
		AccessCount:  m.AccessCount,
	}
}

func entryToPublic(key string, rev uint64, e entry) Entry {
	return Entry{
		Key:      key,
		Revision: rev,
		Meta:     e.M.toPublic(),
	}
}

// bytesReader is a minimal bytes.Reader replacement to avoid extra imports.
type bytesReader struct {
	b []byte
	i int
}

func newBytesReader(b []byte) *bytesReader { return &bytesReader{b: b} }

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, errors.New("EOF")
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
