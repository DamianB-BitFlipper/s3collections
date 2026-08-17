package queue

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"
	"unicode/utf8"
)

func FuzzJobJSON(f *testing.F) {
	f.Add([]byte("hello"), "reason")
	f.Add([]byte{}, "")
	f.Add(bytes.Repeat([]byte("x"), 1<<10), "a very long reason with unicode: é")

	f.Fuzz(func(t *testing.T, payload []byte, reason string) {
		if !utf8.ValidString(reason) {
			t.Skip("reason is not valid UTF-8")
		}
		env := newJobEnvelope("job-1", "q", 7, payload, time.Now(), time.Now())
		env.Reasons = append(env.Reasons, reasonEnvelope{At: time.Now(), Reason: reason})
		data, err := encodeJob(env)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		env2, err := decodeJob(data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if env2.ID != env.ID || env2.Queue != env.Queue || env2.Shard != env.Shard {
			t.Fatal("metadata mismatch")
		}
		got, err := jobPayload(env2)
		if err != nil {
			t.Fatalf("payload decode: %v", err)
		}
		if !bytes.Equal(got, payload) {
			// Re-encode to compare because base64 may normalize padding.
			if base64.StdEncoding.EncodeToString(got) != base64.StdEncoding.EncodeToString(payload) {
				t.Fatal("payload mismatch")
			}
		}
		if len(env2.Reasons) != 1 || env2.Reasons[0].Reason != reason {
			t.Fatal("reason mismatch")
		}
	})
}
