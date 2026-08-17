package cas

import (
	"errors"
	"net/url"
	"path"
	"strings"
	"unicode"
)

// defaultKeyCodec implements reversible URL path-segment escaping.
type defaultKeyCodec struct {
	prefix string
}

// DefaultKeyCodec returns a KeyCodec using URL path-segment escaping.
// It rejects empty keys, control characters, and trailing slashes.
// The returned object key does NOT include the store prefix; callers prepend it.
func DefaultKeyCodec() KeyCodec {
	return &defaultKeyCodec{}
}

// newPrefixKeyCodec returns a KeyCodec bound to a store prefix.
func newPrefixKeyCodec(prefix string) KeyCodec {
	return &defaultKeyCodec{prefix: prefix}
}

// encodeSegment validates a single path segment and applies url.PathEscape.
func encodeSegment(seg string) (string, error) {
	if seg == "" {
		return "", errors.New("cas: empty key segment")
	}
	for _, r := range seg {
		if !unicode.IsPrint(r) {
			return "", errors.New("cas: key contains non-printable character")
		}
	}
	return url.PathEscape(seg), nil
}

func (c *defaultKeyCodec) Encode(appKey string) (string, error) {
	if appKey == "" {
		return "", errors.New("cas: empty key")
	}
	if strings.HasSuffix(appKey, "/") {
		return "", errors.New("cas: key ends with slash")
	}
	if len([]rune(appKey)) > 1024 {
		return "", errors.New("cas: key too long")
	}
	segments := strings.Split(appKey, "/")
	escaped := make([]string, len(segments))
	for i, seg := range segments {
		e, err := encodeSegment(seg)
		if err != nil {
			return "", err
		}
		escaped[i] = e
	}
	return c.prefix + path.Join(escaped...) + objectSuffix, nil
}

func (c *defaultKeyCodec) Decode(objectKey string) (string, error) {
	if !strings.HasPrefix(objectKey, c.prefix) {
		return "", errors.New("cas: object key outside store prefix")
	}
	encoded := strings.TrimPrefix(objectKey, c.prefix)
	if !strings.HasSuffix(encoded, objectSuffix) {
		return "", errors.New("cas: object key missing cas suffix")
	}
	base := strings.TrimSuffix(encoded, objectSuffix)
	segments := strings.Split(base, "/")
	out := make([]string, len(segments))
	for i, seg := range segments {
		d, err := url.PathUnescape(seg)
		if err != nil {
			return "", err
		}
		if d == "" {
			return "", errors.New("cas: empty decoded segment")
		}
		for _, r := range d {
			if !unicode.IsPrint(r) {
				return "", errors.New("cas: decoded key contains non-printable character")
			}
		}
		out[i] = d
	}
	return strings.Join(out, "/"), nil
}
