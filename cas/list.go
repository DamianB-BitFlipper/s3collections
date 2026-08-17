package cas

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/damianb/s3collections/s3backend"
)

// ListOptions controls a Store List call.
type ListOptions struct {
	Prefix            string
	StartAfter        string
	ContinuationToken string
	MaxKeys           int
}

// ListPage is one page of records.
type ListPage struct {
	Records               []Record
	IsTruncated           bool
	NextContinuationToken string
}

// GCOptions controls a garbage-collection sweep.
type GCOptions struct {
	Prefix     string
	OlderThan  time.Time
	MaxDeletes int
}

// encodeListPrefix maps an application-level prefix to a backend object-key
// prefix. To keep listing correct and prefix-safe, we encode complete path
// segments only and truncate at a segment boundary when the prefix ends mid-
// segment. The caller must still filter decoded keys as a safety belt.
func (s *Store) encodeListPrefix(prefix string) string {
	if prefix == "" {
		return s.prefix
	}
	segments := strings.Split(prefix, "/")
	// If the prefix does not end with '/', the last segment is partial; drop
	// it and list from the previous segment boundary. This is conservative:
	// it may return false positives, but never misses a true match.
	if !strings.HasSuffix(prefix, "/") {
		segments = segments[:len(segments)-1]
	}
	escaped := make([]string, 0, len(segments))
	for _, seg := range segments {
		if seg == "" {
			continue
		}
		escaped = append(escaped, url.PathEscape(seg))
	}
	if len(escaped) == 0 {
		return s.prefix
	}
	return s.prefix + strings.Join(escaped, "/") + "/"
}

// encodeListStartAfter maps an application start-after key to a backend
// object key (including the store prefix and .cas.v1.json suffix).
func (s *Store) encodeListStartAfter(startAfter string) (string, error) {
	if startAfter == "" {
		return "", nil
	}
	return s.objectKey(startAfter)
}

// List returns records (including tombstones) whose keys begin with Prefix.
func (s *Store) List(ctx context.Context, opts *ListOptions) (*ListPage, error) {
	start := time.Now()
	if opts == nil {
		opts = &ListOptions{}
	}

	backendPrefix := s.encodeListPrefix(opts.Prefix)
	bopts := &s3backend.ListOptions{
		ContinuationToken: opts.ContinuationToken,
		MaxKeys:           opts.MaxKeys,
	}
	// ContinuationToken takes precedence over StartAfter, matching S3 semantics.
	if opts.ContinuationToken == "" && opts.StartAfter != "" {
		startAfter, err := s.encodeListStartAfter(opts.StartAfter)
		if err != nil {
			s.observeLatency(ctx, opList, start, outcomeError)
			return nil, err
		}
		bopts.StartAfter = startAfter
	}

	page, err := s.backend.List(ctx, backendPrefix, bopts)
	if err != nil {
		s.observeLatency(ctx, opList, start, outcomeError)
		return nil, classifyError(opList, err)
	}
	s.recordListPage(ctx, "cas/store-root")

	records := make([]Record, 0, len(page.Objects))
	for _, info := range page.Objects {
		appKey, err := s.opts.KeyCodec.Decode(info.Key)
		if err != nil {
			s.recordCorrupt(ctx)
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(appKey, opts.Prefix) {
			continue
		}
		if opts.StartAfter != "" && appKey <= opts.StartAfter {
			continue
		}
		// Fetch and parse the envelope.
		obj, err := s.backend.Get(ctx, info.Key)
		if err != nil {
			if errors.Is(err, s3backend.ErrNotFound) {
				continue
			}
			s.observeLatency(ctx, opList, start, outcomeError)
			return nil, classifyError(opList, err)
		}
		e, err := parseEnvelope(obj.Body, appKey)
		if err != nil {
			s.recordCorrupt(ctx)
			continue
		}
		records = append(records, recordFromEnvelope(e, obj.ETag))
	}

	out := &ListPage{
		Records:               records,
		IsTruncated:           page.IsTruncated,
		NextContinuationToken: page.NextContinuationToken,
	}
	s.observeLatency(ctx, opList, start, outcomeSuccess)
	return out, nil
}

// GC physically deletes tombstones older than the retention window.
func (s *Store) GC(ctx context.Context, opts *GCOptions) (int, error) {
	start := time.Now()
	if opts == nil {
		opts = &GCOptions{}
	}
	cutoff := opts.OlderThan
	if cutoff.IsZero() {
		cutoff = time.Now().UTC().Add(-s.opts.TombstoneRetention)
	}
	// Additional clock-skew safety margin: eligible tombstones must be older
	// than the explicit cutoff minus the skew hint.
	cutoff = cutoff.Add(-s.opts.ClockSkewHint)

	deleted := 0
	listToken := ""
	for {
		page, err := s.List(ctx, &ListOptions{
			Prefix:            opts.Prefix,
			ContinuationToken: listToken,
			MaxKeys:           1000,
		})
		if err != nil {
			s.observeLatency(ctx, opGC, start, outcomeError)
			return deleted, err
		}
		for _, rec := range page.Records {
			if rec.State != Tombstone {
				continue
			}
			if rec.DeletedAt.IsZero() || rec.DeletedAt.After(cutoff) {
				continue
			}
			objKey, err := s.objectKey(rec.Key)
			if err != nil {
				continue
			}
			if err := s.backend.Delete(ctx, objKey); err != nil {
				s.observeLatency(ctx, opGC, start, outcomeError)
				return deleted, err
			}
			deleted++
			if opts.MaxDeletes > 0 && deleted >= opts.MaxDeletes {
				s.observeLatency(ctx, opGC, start, outcomeSuccess)
				return deleted, nil
			}
		}
		if !page.IsTruncated {
			break
		}
		listToken = page.NextContinuationToken
		if listToken == "" {
			break
		}
	}
	s.observeLatency(ctx, opGC, start, outcomeSuccess)
	return deleted, nil
}
