package cas

import (
	"strings"
	"testing"
)

func TestKeyCodecRoundTrip(t *testing.T) {
	codec := DefaultKeyCodec()
	cases := []string{
		"simple",
		"with/slash",
		"unicode/日本語",
		"a/b/c/d",
		"space here",
		"percent%20value",
	}
	for _, k := range cases {
		enc, err := codec.Encode(k)
		if err != nil {
			t.Fatalf("encode %q: %v", k, err)
		}
		if !strings.HasSuffix(enc, objectSuffix) {
			t.Fatalf("missing suffix: %q", enc)
		}
		dec, err := codec.Decode(enc)
		if err != nil {
			t.Fatalf("decode %q: %v", enc, err)
		}
		if dec != k {
			t.Fatalf("round trip %q -> %q", k, dec)
		}
	}
}

func TestKeyCodecRejections(t *testing.T) {
	codec := DefaultKeyCodec()
	rejected := []string{
		"",
		"trailing/",
		"foo//bar",
		"control" + "\x00" + "char",
		"tab\tchar",
	}
	for _, k := range rejected {
		if _, err := codec.Encode(k); err == nil {
			t.Fatalf("expected rejection for %q", k)
		}
	}
}
