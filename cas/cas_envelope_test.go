package cas

import (
	"errors"
	"testing"
	"time"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	key := "foo"
	value := []byte("bar")
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	body, err := marshalEnvelope(key, 1, Live, value, now, now, time.Time{}, "w")
	if err != nil {
		t.Fatal(err)
	}
	e, err := parseEnvelope(body, key)
	if err != nil {
		t.Fatal(err)
	}
	if e.State != "live" || e.Rev != 1 || e.ValueB64 != "YmFy" {
		t.Fatalf("unexpected envelope: %+v", e)
	}

	// Corrupt checksum
	body[10] ^= 0xff
	_, err = parseEnvelope(body, key)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
}

func FuzzEnvelopeParse(f *testing.F) {
	now := time.Now().UTC()
	valid, _ := marshalEnvelope("k", 1, Live, []byte("v"), now, now, time.Time{}, "w")
	f.Add(valid)
	f.Add([]byte(`{"v":1,"state":"live","key":"k","rev":1,"value_b64":"dg==","value_sha256":"nothex","created_at":"2026-08-04T12:00:00Z","updated_at":"2026-08-04T12:00:00Z","writer_id":"w"}`))
	f.Add([]byte("not json"))
	f.Add([]byte{})
	f.Add([]byte(`{"v":2,"state":"live","key":"k","rev":1,"value_b64":"dg==","value_sha256":"` + "a" + `","created_at":"2026-08-04T12:00:00Z","updated_at":"2026-08-04T12:00:00Z","writer_id":"w"}`))

	f.Fuzz(func(t *testing.T, body []byte) {
		_, err := parseEnvelope(body, "k")
		if err != nil && !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrNotFound) {
			t.Fatalf("unexpected error type: %v", err)
		}
	})
}
