package cas

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const envelopeVersion = 1

// envelope is the JSON object stored in S3.
type envelope struct {
	V           int       `json:"v"`
	State       string    `json:"state"`
	Key         string    `json:"key"`
	Rev         uint64    `json:"rev"`
	ValueB64    string    `json:"value_b64,omitempty"`
	ValueSHA256 string    `json:"value_sha256,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	DeletedAt   time.Time `json:"deleted_at,omitempty"`
	WriterID    string    `json:"writer_id"`
}

// rawEnvelope mirrors the wire format exactly for strict parsing.
type rawEnvelope struct {
	V           int    `json:"v"`
	State       string `json:"state"`
	Key         string `json:"key"`
	Rev         uint64 `json:"rev"`
	ValueB64    string `json:"value_b64"`
	ValueSHA256 string `json:"value_sha256"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	DeletedAt   string `json:"deleted_at"`
	WriterID    string `json:"writer_id"`
}

// recordFromEnvelope converts a parsed envelope into a public Record.
func recordFromEnvelope(e *envelope, etag string) Record {
	rec := Record{
		Key:       e.Key,
		Revision:  e.Rev,
		CreatedAt: e.CreatedAt,
		UpdatedAt: e.UpdatedAt,
		DeletedAt: e.DeletedAt,
		WriterID:  e.WriterID,
		ETag:      etag,
	}
	switch e.State {
	case "live":
		rec.State = Live
		rec.Value, _ = base64.StdEncoding.DecodeString(e.ValueB64)
	case "tombstone":
		rec.State = Tombstone
	}
	return rec
}

// marshalEnvelope serializes a record envelope.
func marshalEnvelope(key string, rev uint64, state State, value []byte, createdAt, updatedAt, deletedAt time.Time, writerID string) ([]byte, error) {
	e := envelope{
		V:         envelopeVersion,
		Key:       key,
		Rev:       rev,
		CreatedAt: createdAt.UTC(),
		UpdatedAt: updatedAt.UTC(),
		WriterID:  writerID,
	}
	switch state {
	case Live:
		e.State = "live"
		e.ValueB64 = base64.StdEncoding.EncodeToString(value)
		sum := sha256.Sum256(value)
		e.ValueSHA256 = hex.EncodeToString(sum[:])
	case Tombstone:
		e.State = "tombstone"
		e.DeletedAt = deletedAt.UTC()
	default:
		return nil, errors.New("cas: invalid state")
	}
	return json.Marshal(e)
}

// parseEnvelope decodes and validates an envelope body.
// appKey is the requested application key; a mismatch yields ErrCorrupt.
func parseEnvelope(body []byte, appKey string) (*envelope, error) {
	var raw rawEnvelope
	dec := json.NewDecoder(bytesNewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrCorrupt, err)
	}
	if raw.V != envelopeVersion {
		return nil, fmt.Errorf("%w: unsupported envelope version %d", ErrCorrupt, raw.V)
	}
	if raw.Key != appKey {
		return nil, fmt.Errorf("%w: stored key %q does not match requested key %q", ErrCorrupt, raw.Key, appKey)
	}

	e := &envelope{
		V:        raw.V,
		State:    raw.State,
		Key:      raw.Key,
		Rev:      raw.Rev,
		WriterID: raw.WriterID,
	}

	var err error
	e.CreatedAt, err = parseTime(raw.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("%w: created_at: %w", ErrCorrupt, err)
	}
	e.UpdatedAt, err = parseTime(raw.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("%w: updated_at: %w", ErrCorrupt, err)
	}

	switch raw.State {
	case "live":
		if raw.ValueB64 == "" || raw.ValueSHA256 == "" {
			return nil, fmt.Errorf("%w: live envelope missing value fields", ErrCorrupt)
		}
		value, err := base64.StdEncoding.DecodeString(raw.ValueB64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid value_b64: %w", ErrCorrupt, err)
		}
		expected := sha256Bytes(value)
		if !constantTimeEqual(expected, raw.ValueSHA256) {
			return nil, fmt.Errorf("%w: value_sha256 mismatch", ErrCorrupt)
		}
		e.ValueB64 = raw.ValueB64
		e.ValueSHA256 = raw.ValueSHA256
	case "tombstone":
		if raw.DeletedAt == "" {
			return nil, fmt.Errorf("%w: tombstone missing deleted_at", ErrCorrupt)
		}
		e.DeletedAt, err = parseTime(raw.DeletedAt)
		if err != nil {
			return nil, fmt.Errorf("%w: deleted_at: %w", ErrCorrupt, err)
		}
		if raw.ValueB64 != "" || raw.ValueSHA256 != "" {
			return nil, fmt.Errorf("%w: tombstone must not contain value fields", ErrCorrupt)
		}
	default:
		return nil, fmt.Errorf("%w: invalid state %q", ErrCorrupt, raw.State)
	}
	return e, nil
}

func parseTime(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func sha256Bytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// bytesNewReader avoids importing bytes just for this helper.
func bytesNewReader(b []byte) *bytesReader {
	return &bytesReader{b: b, i: 0}
}

type bytesReader struct {
	b []byte
	i int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, errors.New("EOF")
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

// constantTimeEqual compares two hex strings using constant-time equality on bytes.
func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}
